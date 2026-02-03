
#include "textflag.h"

// func ScanSeparatorsAVX2(data []byte, sep byte) uint64
TEXT ·ScanSeparatorsAVX2(SB), NOSPLIT, $8-32
    MOVQ    data_base+0(FP), SI    // SI = data base pointer
    MOVQ    data_len+8(FP), CX     // CX = data length
    MOVB    sep+24(FP), AL         // AL = separator byte

    XORQ    R8, R8                 // R8 = count (Result)
    TESTQ   CX, CX
    JZ      ret_avx2

    // Broadcast separator to Y0 using stack temp
    MOVB    AL, tmp-1(SP)          // Move sep to stack
    VPBROADCASTB tmp-1(SP), Y0     // Broadcast from memory to Y0

    // Check if less than 32 bytes
    CMPQ    CX, $32
    JB      tail_loop_avx2

    // Main loop (32 bytes per iteration)
loop_avx2:
    CMPQ    CX, $32
    JB      tail_loop_avx2

    VMOVDQU (SI), Y1               // Load 32 bytes
    VPCMPEQB Y0, Y1, Y1            // Compare against separator
    VPMOVMSKB Y1, R9               // Extract mask to scalar R9

    POPCNTL R9, R9                 // Count set bits
    ADDQ    R9, R8                 // Add to total count

    ADDQ    $32, SI                // Advance pointer
    SUBQ    $32, CX                // Decrement length
    JMP     loop_avx2

tail_loop_avx2:
    TESTQ   CX, CX
    JZ      ret_avx2
    
    // Scalar fallback for tail < 32 bytes
    MOVB    (SI), DL
    CMPB    DL, AL
    JNE     next_tail_avx2
    INCQ    R8
next_tail_avx2:
    INCQ    SI
    DECQ    CX
    JMP     tail_loop_avx2

ret_avx2:
    MOVQ    R8, ret+32(FP)
    RET


// func ScanSeparatorsAVX512(data []byte, sep byte) uint64
// Requirements: AVX512F, AVX512BW
TEXT ·ScanSeparatorsAVX512(SB), NOSPLIT, $8-32
    MOVQ    data_base+0(FP), SI    // SI = data base pointer
    MOVQ    data_len+8(FP), CX     // CX = data length
    MOVB    sep+24(FP), AL         // AL = separator byte

    XORQ    R8, R8                 // R8 = count (Result)
    TESTQ   CX, CX
    JZ      ret_avx512

    // Broadcast separator to Z0 (512-bit)
    MOVB    AL, tmp-1(SP)
    VPBROADCASTB tmp-1(SP), Z0

    // Unrolled Header Loop
    // Process 256 bytes per iteration (64 * 4)
unrolled_loop_start:
    CMPQ    CX, $256
    JB      single_block_loop

    // Prefetch ahead
    PREFETCHT0 1024(SI)

    // Block 1
    VMOVDQU64 (SI), Z1             
    VPCMPB    $0, Z0, Z1, K1        // Compare equal ($0) to mask K1
    KMOVQ     K1, R9                
    POPCNTQ   R9, R9                
    ADDQ      R9, R8

    // Block 2
    VMOVDQU64 64(SI), Z2
    VPCMPB    $0, Z0, Z2, K2
    KMOVQ     K2, R9
    POPCNTQ   R9, R9
    ADDQ      R9, R8

    // Block 3
    VMOVDQU64 128(SI), Z3
    VPCMPB    $0, Z0, Z3, K3
    KMOVQ     K3, R9
    POPCNTQ   R9, R9
    ADDQ      R9, R8

    // Block 4
    VMOVDQU64 192(SI), Z4
    VPCMPB    $0, Z0, Z4, K4
    KMOVQ     K4, R9
    POPCNTQ   R9, R9
    ADDQ      R9, R8

    ADDQ    $256, SI
    SUBQ    $256, CX
    JMP     unrolled_loop_start

single_block_loop:
    CMPQ    CX, $64
    JB      tail_avx512

    VMOVDQU64 (SI), Z1
    VPCMPB    $0, Z0, Z1, K1
    KMOVQ     K1, R9
    POPCNTQ   R9, R9
    ADDQ      R9, R8

    ADDQ    $64, SI
    SUBQ    $64, CX
    JMP     single_block_loop

tail_avx512:
    TESTQ   CX, CX
    JZ      ret_avx512

    // Create a bitmask of '1's for the first CX bits.
    // K7 = (1 << CX) - 1
    MOVQ    $1, R11
    SHLQ    CX, R11     // R11 = 1 << CX
    DECQ    R11         // R11 = (1 << CX) - 1
    
    KMOVQ   R11, K7     // Move mask to K7
    
    // Masked Compare attempt
    // Load 64 bytes (potentially unsafe, but often accepted in high perf if pages allow)
    // To strictly avoid it without masked-load support in Go ASM is hard.
    // Assuming we can read:
    
    VMOVDQU8 (SI), Z1    // Load 64 bytes
    VPCMPB   $0, Z0, Z1, K1 // Compare all 64 to K1
    KANDQ    K1, K7, K1  // Apply length mask: K1 = K1 & K7
    KMOVQ    K1, R9
    POPCNTQ  R9, R9
    ADDQ     R9, R8
    
ret_avx512:
    MOVQ    R8, ret+32(FP)
    RET
