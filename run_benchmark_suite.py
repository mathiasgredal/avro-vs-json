#!/usr/bin/env python3
"""
Run comprehensive benchmark suite and generate visualizations
"""

import subprocess
import json
import sys
import os
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import numpy as np

# Ensure we're in the project root
os.chdir(Path(__file__).parent)

def run_benchmark(lang, mode, messages, batch, compression, brokers="localhost:9094", large=False):
    """Run a single benchmark and return parsed results"""
    size_label = "large" if large else "small"
    print(f"Running: {lang} {mode} messages={messages} batch={batch} compression={compression} size={size_label}")
    
    if lang == "node":
        cmd = [
            "node", "node/bench.js",
            "--mode", mode,
            "--messages", str(messages),
            "--batch", str(batch),
            "--compression", compression,
            "--brokers", brokers
        ]
        if large:
            cmd.append("--large")
    elif lang == "go":
        cmd = [
            "./go/bench",
            "--mode", mode,
            "--messages", str(messages),
            "--batch", str(batch),
            "--compression", compression,
            "--brokers", brokers
        ]
        if large:
            cmd.append("--large")
    else:  # python
        cmd = [
            ".venv/bin/python", "python/bench.py",
            "--mode", mode,
            "--messages", str(messages),
            "--batch", str(batch),
            "--compression", compression,
            "--brokers", brokers
        ]
        if large:
            cmd.append("--large")
    
    try:
        my_env = os.environ.copy()
        my_env["GOMAXPROCS"] = "1"
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=my_env)
        if result.returncode != 0:
            print(f"  ERROR: {result.stderr}")
            return None
        
        # Parse JSON output
        output = result.stdout.strip()
        data = json.loads(output)
        print(f"  ✓ Produced {data['produced']} msgs in {data['durationSec']:.2f}s ({data['rate']:.0f} msg/s)")
        return data
    except subprocess.TimeoutExpired:
        print(f"  ERROR: Benchmark timed out")
        return None
    except json.JSONDecodeError as e:
        print(f"  ERROR: Failed to parse JSON: {e}")
        print(f"  Output was: {result.stdout}")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def check_kafka():
    """Check if Kafka is running"""
    print("Checking Kafka status...")
    result = subprocess.run(["docker", "compose", "ps"], capture_output=True, text=True)
    if "kafka" not in result.stdout or "Up" not in result.stdout:
        print("⚠️  Kafka doesn't appear to be running. Starting with 'make up'...")
        subprocess.run(["make", "up"], check=True)
    else:
        print("✓ Kafka is running")


def build_go_binary():
    """Build Go binary once before benchmarks"""
    print("Building Go binary...")
    result = subprocess.run(
        ["go", "build", "-o", "bench", "bench.go"],
        cwd="go",
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"✗ Failed to build Go binary: {result.stderr}")
        sys.exit(1)
    print("✓ Go binary built")


def plot_compression_comparison(results, output_file="results/compression_comparison.png"):
    """Plot bar chart comparing different compression types"""
    if os.path.exists(output_file):
        print(f"⏭️  Skipping {output_file} (already exists)")
        return
    
    compressions = sorted(set(r['compression'] for r in results))
    
    # Group by lang, mode, compression
    data = {}
    for r in results:
        key = (r['lang'], r['mode'])
        if key not in data:
            data[key] = {}
        data[key][r['compression']] = r['rate']
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = np.arange(len(compressions))
    width = 0.12
    
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c']
    patterns = ['', '///', '...', 'xxx', '\\\\\\', '+++']
    
    bars = []
    labels = []
    
    for idx, (key, color, pattern) in enumerate(zip(sorted(data.keys()), colors, patterns)):
        lang, mode = key
        rates = [data[key].get(comp, 0) for comp in compressions]
        offset = width * (idx - 2.5)
        bar = ax.bar(x + offset, rates, width, label=f'{lang}-{mode}', 
                     color=color, hatch=pattern, edgecolor='black', linewidth=0.5)
        bars.append(bar)
        labels.append(f'{lang}-{mode}')
    
    ax.set_xlabel('Compression Type', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (messages/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Kafka Producer Throughput by Compression Type', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(compressions)
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars
    for bar in bars:
        for rect in bar:
            height = rect.get_height()
            if height > 0:
                ax.text(rect.get_x() + rect.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Saved plot: {output_file}")


def plot_batch_size_comparison(results, output_file="results/batch_size_comparison.png"):
    """Plot bar chart comparing different batch sizes"""
    if os.path.exists(output_file):
        print(f"⏭️  Skipping {output_file} (already exists)")
        return
    
    batch_sizes = sorted(set(r['batch'] for r in results))
    
    # Group by lang, mode, batch
    data = {}
    for r in results:
        key = (r['lang'], r['mode'])
        if key not in data:
            data[key] = {}
        data[key][r['batch']] = r['rate']
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = np.arange(len(batch_sizes))
    width = 0.12
    
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c']
    patterns = ['', '///', '...', 'xxx', '\\\\\\', '+++']
    
    bars = []
    labels = []
    
    for idx, (key, color, pattern) in enumerate(zip(sorted(data.keys()), colors, patterns)):
        lang, mode = key
        rates = [data[key].get(batch, 0) for batch in batch_sizes]
        offset = width * (idx - 2.5)
        bar = ax.bar(x + offset, rates, width, label=f'{lang}-{mode}', 
                     color=color, hatch=pattern, edgecolor='black', linewidth=0.5)
        bars.append(bar)
        labels.append(f'{lang}-{mode}')
    
    ax.set_xlabel('Batch Size', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (messages/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Kafka Producer Throughput by Batch Size', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in batch_sizes])
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars
    for bar in bars:
        for rect in bar:
            height = rect.get_height()
            if height > 0:
                ax.text(rect.get_x() + rect.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved plot: {output_file}")


def plot_message_count_comparison(results, output_file="results/message_count_comparison.png"):
    """Plot bar chart comparing different message counts"""
    if os.path.exists(output_file):
        print(f"⏭️  Skipping {output_file} (already exists)")
        return
    
    message_counts = sorted(set(r['messages'] for r in results))
    
    # Group by lang, mode, messages
    data = {}
    for r in results:
        key = (r['lang'], r['mode'])
        if key not in data:
            data[key] = {}
        data[key][r['messages']] = r['rate']
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = np.arange(len(message_counts))
    width = 0.12
    
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c']
    patterns = ['', '///', '...', 'xxx', '\\\\\\', '+++']
    
    bars = []
    labels = []
    
    for idx, (key, color, pattern) in enumerate(zip(sorted(data.keys()), colors, patterns)):
        lang, mode = key
        rates = [data[key].get(msg, 0) for msg in message_counts]
        offset = width * (idx - 2.5)
        bar = ax.bar(x + offset, rates, width, label=f'{lang}-{mode}', 
                     color=color, hatch=pattern, edgecolor='black', linewidth=0.5)
        bars.append(bar)
        labels.append(f'{lang}-{mode}')
    
    ax.set_xlabel('Number of Messages', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (messages/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Kafka Producer Throughput by Message Count', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{m:,}' for m in message_counts])
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars
    for bar in bars:
        for rect in bar:
            height = rect.get_height()
            if height > 0:
                ax.text(rect.get_x() + rect.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved plot: {output_file}")


def plot_message_size_comparison(results, output_file="results/message_size_comparison.png"):
    """Plot bar chart comparing large vs small message sizes"""
    if os.path.exists(output_file):
        print(f"⏭️  Skipping {output_file} (already exists)")
        return
    
    # Group by lang, mode, large flag
    data = {}
    for r in results:
        key = (r['lang'], r['mode'])
        if key not in data:
            data[key] = {}
        size_label = 'Large (1000x)' if r.get('large', False) else 'Small (default)'
        data[key][size_label] = r['rate']
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    size_types = ['Small (default)', 'Large (1000x)']
    x = np.arange(len(size_types))
    width = 0.12
    
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c']
    patterns = ['', '///', '...', 'xxx', '\\\\\\', '+++']
    
    bars = []
    labels = []
    
    for idx, (key, color, pattern) in enumerate(zip(sorted(data.keys()), colors, patterns)):
        lang, mode = key
        rates = [data[key].get(size, 0) for size in size_types]
        offset = width * (idx - 2.5)
        bar = ax.bar(x + offset, rates, width, label=f'{lang}-{mode}', 
                     color=color, hatch=pattern, edgecolor='black', linewidth=0.5)
        bars.append(bar)
        labels.append(f'{lang}-{mode}')
    
    ax.set_xlabel('Message Size', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (messages/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Kafka Producer Throughput by Message Size', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(size_types)
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars
    for bar in bars:
        for rect in bar:
            height = rect.get_height()
            if height > 0:
                ax.text(rect.get_x() + rect.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved plot: {output_file}")


def plot_overall_comparison(results, output_file="results/overall_comparison.png"):
    """Plot overall comparison of all configurations"""
    if os.path.exists(output_file):
        print(f"⏭️  Skipping {output_file} (already exists)")
        return
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Group results by lang-mode combination
    data = {}
    for r in results:
        key = f"{r['lang']}-{r['mode']}"
        if key not in data:
            data[key] = []
        data[key].append(r['rate'])
    
    # Calculate averages
    categories = sorted(data.keys())
    averages = [np.mean(data[cat]) for cat in categories]
    maxes = [np.max(data[cat]) for cat in categories]
    mins = [np.min(data[cat]) for cat in categories]
    
    x = np.arange(len(categories))
    width = 0.5
    
    colors = {'node-avro': '#3498db', 'node-json': '#2ecc71', 
              'python-avro': '#e74c3c', 'python-json': '#f39c12',
              'go-avro': '#9b59b6', 'go-json': '#1abc9c'}
    
    bars = ax.bar(x, averages, width, 
                  color=[colors.get(cat, '#95a5a6') for cat in categories],
                  edgecolor='black', linewidth=1)
    
    # Add error bars for min/max range
    errors = [[avg - min_val for avg, min_val in zip(averages, mins)],
              [max_val - avg for avg, max_val in zip(averages, maxes)]]
    ax.errorbar(x, averages, yerr=errors, fmt='none', ecolor='black', 
                capsize=5, capthick=2, alpha=0.7)
    
    ax.set_xlabel('Configuration', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (messages/sec)', fontsize=12, fontweight='bold')
    ax.set_title('Average Kafka Producer Throughput (with min/max range)', 
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=0)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels
    for i, (bar, avg) in enumerate(zip(bars, averages)):
        ax.text(bar.get_x() + bar.get_width()/2., avg,
               f'{int(avg)}\n(avg)',
               ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved plot: {output_file}")


def main():
    print("=" * 80)
    print("KAFKA BENCHMARK SUITE")
    print("=" * 80)
    
    # Check Kafka
    check_kafka()
    
    # Build Go binary once
    build_go_binary()
    
    # Define benchmark configurations
    languages = ["node", "python", "go"]
    modes = ["json", "avro"]
    
    all_results = []
    
    # Ensure results directory exists
    os.makedirs("results", exist_ok=True)
    
    # Test 1: Compression comparison (fixed messages=50000, batch=100)
    compression_file = "results/compression_comparison.png"
    if not os.path.exists(compression_file):
        print("\n" + "=" * 80)
        print("TEST 1: Compression Comparison")
        print("=" * 80)
        compressions = ["none", "gzip", "snappy"]
        compression_results = []
        
        for lang in languages:
            for mode in modes:
                for comp in compressions:
                    result = run_benchmark(lang, mode, messages=50000, batch=100, compression=comp)
                    if result:
                        result['compression'] = comp
                        result['batch'] = 100
                        result['messages'] = 50000
                        result['large'] = False
                        compression_results.append(result)
                        all_results.append(result)
        
        if compression_results:
            plot_compression_comparison(compression_results)
    else:
        print(f"\n⏭️  Skipping TEST 1: Compression Comparison ({compression_file} exists)")
    
    # Test 2: Batch size comparison (fixed messages=50000, compression=snappy)
    batch_file = "results/batch_size_comparison.png"
    if not os.path.exists(batch_file):
        print("\n" + "=" * 80)
        print("TEST 2: Batch Size Comparison")
        print("=" * 80)
        batch_sizes = [10, 50, 100, 500, 1000]
        batch_results = []
        
        for lang in languages:
            for mode in modes:
                for batch in batch_sizes:
                    result = run_benchmark(lang, mode, messages=50000, batch=batch, compression="snappy")
                    if result:
                        result['compression'] = "snappy"
                        result['batch'] = batch
                        result['messages'] = 50000
                        result['large'] = False
                        batch_results.append(result)
                        all_results.append(result)
        
        if batch_results:
            plot_batch_size_comparison(batch_results)
    else:
        print(f"\n⏭️  Skipping TEST 2: Batch Size Comparison ({batch_file} exists)")
    
    # Test 3: Message count comparison (fixed batch=100, compression=snappy)
    message_count_file = "results/message_count_comparison.png"
    if not os.path.exists(message_count_file):
        print("\n" + "=" * 80)
        print("TEST 3: Message Count Comparison")
        print("=" * 80)
        message_counts = [10000, 50000, 100000]
        message_results = []
        
        for lang in languages:
            for mode in modes:
                for msg_count in message_counts:
                    result = run_benchmark(lang, mode, messages=msg_count, batch=100, compression="snappy")
                    if result:
                        result['compression'] = "snappy"
                        result['batch'] = 100
                        result['messages'] = msg_count
                        result['large'] = False
                        message_results.append(result)
                        all_results.append(result)
        
        if message_results:
            plot_message_count_comparison(message_results)
    else:
        print(f"\n⏭️  Skipping TEST 3: Message Count Comparison ({message_count_file} exists)")
    
    # Test 4: Message size comparison (fixed messages=10000, batch=50, compression=gzip)
    message_size_file = "results/message_size_comparison.png"
    if not os.path.exists(message_size_file):
        print("\n" + "=" * 80)
        print("TEST 4: Message Size Comparison (Small vs Large)")
        print("=" * 80)
        size_results = []
        
        for lang in languages:
            for mode in modes:
                for is_large in [False, True]:
                    result = run_benchmark(lang, mode, messages=10000, batch=50, compression="gzip", large=is_large)
                    if result:
                        result['compression'] = "gzip"
                        result['batch'] = 50
                        result['messages'] = 10000
                        result['large'] = is_large
                        size_results.append(result)
                        all_results.append(result)
        
        if size_results:
            plot_message_size_comparison(size_results)
    else:
        print(f"\n⏭️  Skipping TEST 4: Message Size Comparison ({message_size_file} exists)")
    
    # Overall comparison
    if all_results:
        plot_overall_comparison(all_results)
        
        # Save raw results
        with open('results/benchmark_results.json', 'w') as f:
            json.dump(all_results, f, indent=2)
        print(f"\n✓ Saved raw results: results/benchmark_results.json")
    
    print("\n" + "=" * 80)
    print("BENCHMARK SUITE COMPLETE")
    print("=" * 80)
    print(f"Total benchmarks run: {len(all_results)}")
    print("\nGenerated plots:")
    print("  - results/compression_comparison.png")
    print("  - results/batch_size_comparison.png")
    print("  - results/message_count_comparison.png")
    print("  - results/message_size_comparison.png")
    print("  - results/overall_comparison.png")


if __name__ == "__main__":
    main()
