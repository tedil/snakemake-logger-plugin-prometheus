import subprocess
import time
import requests
import pytest

SNAKEFILE_CONTENT = """
rule all:
    input: "output.txt"

rule sleeper:
    output: "output.txt"
    threads: 2
    resources:
        mem_mb=500
    shell:
        "sleep 3 && touch {output}"
"""


@pytest.fixture
def workdir(tmp_path):
    sf = tmp_path / "Snakefile"
    sf.write_text(SNAKEFILE_CONTENT)
    return tmp_path


def test_integration_snakemake_run(workdir):
    """
    Runs snakemake with the prometheus logger and queries the metrics endpoint.
    Waits until the job actually starts to verify metrics.
    """

    test_port = 18001
    snakefile_path = workdir / "Snakefile"

    cmd = [
        "snakemake",
        "--logger",
        "prometheus",
        "--logger-prometheus-port",
        str(test_port),
        "--cores",
        "1",
        "--directory",
        str(workdir),
        "--snakefile",
        str(snakefile_path),
    ]

    # Start Snakemake
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    try:
        start_time = time.time()
        metric_found = False
        timeout = 15

        while time.time() - start_time < timeout:
            # check if process dies early
            if proc.poll() is not None:
                outs, errs = proc.communicate()
                pytest.fail(f"Snakemake process finished prematurely.\nSTDOUT: {outs}\nSTDERR: {errs}")

            try:
                response = requests.get(f"http://localhost:{test_port}/metrics")
                if response.status_code == 200:
                    metrics = response.text

                    if 'snakemake_jobs_running{rule="sleeper"}' in metrics:
                        metric_found = True
                        break
            except requests.exceptions.ConnectionError:
                pass

            time.sleep(0.5)

        if not metric_found:
            try:
                metrics = requests.get(f"http://localhost:{test_port}/metrics").text
            except:
                metrics = "Could not retrieve metrics"

            proc.terminate()
            outs, errs = proc.communicate()
            pytest.fail(f"Timeout waiting for 'sleeper' metric.\nLast Metrics:\n{metrics}\nSnakemake Stderr:\n{errs}")

        assert metric_found

    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    if proc.returncode == 0:
        assert (workdir / "output.txt").exists()
