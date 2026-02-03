//go:build amd64
// +build amd64

#include "textflag.h"

// func checkAVX2() bool
TEXT ·checkAVX2(SB), NOSPLIT, $0-1
	MOVQ $7, AX
	MOVQ $0, CX
	CPUID
	// Check EBX bit 5 (AVX2)
	SHRQ $5, BX
	ANDQ $1, BX
	MOVB BX, ret+0(FP)
	RET

// func checkSSE42() bool
TEXT ·checkSSE42(SB), NOSPLIT, $0-1
	MOVQ $1, AX
	CPUID
	// Check ECX bit 20 (SSE4.2)
	SHRQ $20, CX
	ANDQ $1, CX
	MOVB CX, ret+0(FP)
	RET

// Constants
DATA mask_quote<>+0(SB)/8, $0x2222222222222222
DATA mask_quote<>+8(SB)/8, $0x2222222222222222
DATA mask_comma<>+0(SB)/8, $0x2C2C2C2C2C2C2C2C
DATA mask_comma<>+8(SB)/8, $0x2C2C2C2C2C2C2C2C
DATA mask_newline<>+0(SB)/8, $0x0A0A0A0A0A0A0A0A
DATA mask_newline<>+8(SB)/8, $0x0A0A0A0A0A0A0A0A

GLOBL mask_quote<>(SB), RODATA, $16
GLOBL mask_comma<>(SB), RODATA, $16
GLOBL mask_newline<>(SB), RODATA, $16

// func scanAVX2(data unsafe.Pointer, len int, quotes, commas, newlines unsafe.Pointer) int
// Process 64 bytes per iteration
TEXT ·scanAVX2(SB), NOSPLIT, $0-48
	MOVQ data+0(FP), SI       // SI = src pointer
	MOVQ len+8(FP), DX        // DX = length
	MOVQ quotes+16(FP), DI    // DI = quotes dst
	MOVQ commas+24(FP), CX    // CX = commas dst
	MOVQ newlines+32(FP), R8  // R8 = newlines dst

	// Prepare constants in YMM registers
	// AVX2 requires broadcasting from an XMM register or memory, 
	// not directly from a GPR (that is AVX-512).
	
	MOVQ $0x22, AX
	VMOVD AX, X1
	VPBROADCASTB X1, Y1       // Y1 = quotes (AVX2 compatible)
	
	MOVQ $0x2C, AX
	VMOVD AX, X2
	VPBROADCASTB X2, Y2       // Y2 = commas
	
	MOVQ $0x0A, AX
	VMOVD AX, X3
	VPBROADCASTB X3, Y3       // Y3 = newlines

	MOVQ $0, R9               // R9 = processed count

loop_avx2:
	CMPQ DX, $64
	JL done_avx2              // If < 64 bytes left, finish

	// Load 32 bytes (Batch 1)
	VMOVDQU (SI), Y0
	
	// Compare Quotes
	VPCMPEQB Y1, Y0, Y4
	VPMOVMSKB Y4, AX          // AX = quote mask 1 (32 bits)

	// Compare Commas
	VPCMPEQB Y2, Y0, Y4
	VPMOVMSKB Y4, BX          // BX = comma mask 1

	// Compare Newlines
	VPCMPEQB Y3, Y0, Y4
	VPMOVMSKB Y4, R10         // R10 = newline mask 1

	// Load next 32 bytes (Batch 2)
	VMOVDQU 32(SI), Y0

	// Compare Quotes
	VPCMPEQB Y1, Y0, Y4
	VPMOVMSKB Y4, R11         // R11 = quote mask 2
	
	// Combine Quote Masks
	SHLQ $32, R11
	ORQ R11, AX
	MOVQ AX, (DI)             // Store uint64 quotes

	// Compare Commas
	VPCMPEQB Y2, Y0, Y4
	VPMOVMSKB Y4, R11         // R11 = comma mask 2

	// Combine Comma Masks
	SHLQ $32, R11
	ORQ R11, BX
	MOVQ BX, (CX)             // Store uint64 commas

	// Compare Newlines
	VPCMPEQB Y3, Y0, Y4
	VPMOVMSKB Y4, R11         // R11 = newline mask 2
	
	// Combine Newline Masks
	SHLQ $32, R11
	ORQ R11, R10
	MOVQ R10, (R8)            // Store uint64 newlines

	// Advance pointers
	ADDQ $64, SI
	ADDQ $8, DI
	ADDQ $8, CX
	ADDQ $8, R8
	SUBQ $64, DX
	ADDQ $64, R9
	JMP loop_avx2

done_avx2:
	// Return processed count
	MOVQ R9, ret+40(FP)
	VZEROUPPER
	RET

// func scanSSE42(data unsafe.Pointer, len int, quotes, commas, newlines unsafe.Pointer) int
TEXT ·scanSSE42(SB), NOSPLIT, $0-48
	MOVQ data+0(FP), SI
	MOVQ len+8(FP), DX
	MOVQ quotes+16(FP), DI
	MOVQ commas+24(FP), CX
	MOVQ newlines+32(FP), R8

	// Prepare constants XMM
	MOVOU mask_quote<>(SB), X1
	MOVOU mask_comma<>(SB), X2
	MOVOU mask_newline<>(SB), X3

	MOVQ $0, R9 // Processed count

loop_sse:
	CMPQ DX, $64
	JL done_sse

	// Unroll 4x 16 bytes = 64 bytes
	// Block 0
	MOVOU (SI), X0
	MOVO  X0, X4; PCMPEQB X1, X4; PMOVMSKB X4, AX  // Quote
	MOVO  X0, X4; PCMPEQB X2, X4; PMOVMSKB X4, BX  // Comma
	MOVO  X0, X4; PCMPEQB X3, X4; PMOVMSKB X4, R10 // Newline

	// Block 1
	MOVOU 16(SI), X0
	MOVO  X0, X4; PCMPEQB X1, X4; PMOVMSKB X4, R11
	SHLQ $16, R11; ORQ R11, AX
	
	MOVO  X0, X4; PCMPEQB X2, X4; PMOVMSKB X4, R11
	SHLQ $16, R11; ORQ R11, BX
	
	MOVO  X0, X4; PCMPEQB X3, X4; PMOVMSKB X4, R11
	SHLQ $16, R11; ORQ R11, R10

	// Block 2
	MOVOU 32(SI), X0
	MOVO  X0, X4; PCMPEQB X1, X4; PMOVMSKB X4, R11
	SHLQ $32, R11; ORQ R11, AX
	
	MOVO  X0, X4; PCMPEQB X2, X4; PMOVMSKB X4, R11
	SHLQ $32, R11; ORQ R11, BX
	
	MOVO  X0, X4; PCMPEQB X3, X4; PMOVMSKB X4, R11
	SHLQ $32, R11; ORQ R11, R10

	// Block 3
	MOVOU 48(SI), X0
	MOVO  X0, X4; PCMPEQB X1, X4; PMOVMSKB X4, R11
	SHLQ $48, R11; ORQ R11, AX
	
	MOVO  X0, X4; PCMPEQB X2, X4; PMOVMSKB X4, R11
	SHLQ $48, R11; ORQ R11, BX
	
	MOVO  X0, X4; PCMPEQB X3, X4; PMOVMSKB X4, R11
	SHLQ $48, R11; ORQ R11, R10

	// Store
	MOVQ AX, (DI)
	MOVQ BX, (CX)
	MOVQ R10, (R8)

	// Advance
	ADDQ $64, SI
	ADDQ $8, DI
	ADDQ $8, CX
	ADDQ $8, R8
	SUBQ $64, DX
	ADDQ $64, R9
	JMP loop_sse

done_sse:
	MOVQ R9, ret+40(FP)
	RET
