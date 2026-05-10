import subprocess, sys, os

scripts = [
    "2_1_top_k.py",
    "2_2_word_region.py",
    "2_3_kl_divergence.py",
    "2_4_peaks.py"
]

script_dir = os.path.dirname(os.path.abspath(__file__))

for script in scripts:
    print(f"Ejecutando {script}...")
    result = subprocess.run([sys.executable, os.path.join(script_dir, script)], check=True)

print("\n✅ Pipeline MapReduce completo.")