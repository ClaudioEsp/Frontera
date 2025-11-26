import subprocess

def run_command(command: str):
    """Executes a given shell command."""
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Command '{command}' succeeded with output:\n{result.stdout.decode()}")
    except subprocess.CalledProcessError as e:
        print(f"Command '{command}' failed with error:\n{e.stderr.decode()}")
        raise

def main():
    # List of commands to run in sequence
    commands = [
        "python fetch_dispatches.py",
        "python -m backfill_promise_date_from_tags",
        "python -m backfill_tipo_orden_from_tags",
        "python -m backfill_ct",
        "python -m backfill_substatus"
    ]
    
    # Run each command in sequence
    for command in commands:
        run_command(command)

if __name__ == "__main__":
    main()
