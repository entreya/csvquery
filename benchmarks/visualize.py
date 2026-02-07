import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def generate_chart(csv_file='results.csv', output_file='benchmark_viz.png'):
    # Load hyperfine CSV export
    df = pd.read_csv(csv_file)
    
    # Setup styling for "Dark Mode" / LinkedIn Aesthetic
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Colors for the bars
    colors = ['#6272a4', '#50fa7b'] # Dracula-ish colors (Purple for PHP, Green for Go)
    
    # Create horizontal bar chart
    bars = sns.barplot(
        x='mean', 
        y='command', 
        data=df, 
        palette=colors,
        ax=ax
    )
    
    # Customize text and labels
    ax.set_title('CsvQuery (Go) vs Native PHP (fgetcsv)', fontsize=18, fontweight='bold', pad=20, color='#f8f8f2')
    ax.set_xlabel('Mean Execution Time (seconds)', fontsize=12, color='#6272a4')
    ax.set_ylabel('', color='#f8f8f2')
    
    # Add data labels
    for i, v in enumerate(df['mean']):
        ax.text(v + 0.01, i, f"{v:.3f}s", color='#f8f8f2', va='center', fontweight='bold')

    # Remove spines for clean look
    sns.despine(left=True, bottom=True)
    ax.grid(axis='x', linestyle='--', alpha=0.2)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✅ Visualization saved to {output_file}")

if __name__ == "__main__":
    generate_chart()
