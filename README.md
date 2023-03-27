# miniflow

Airflow like workflow automation for the command prompt. 

    Usage: miniflow <command> [args...]

    COMMANDS

       run                       Run the flow, attempting to make progress based on the current state.
       reset [--only] [step]...  Reset a step + its dependents. If no step is provided, reset all steps
       purge                     Clear all state associated with this flow and start from scratch
       status                    Show short-form flow status
       inspect                   Show detailed information about the flow for debugging
       logs [step]               Print logs from the last run of a step

    OPTIONS

        -d --flow <miniflow.toml>  Specify the flow file to use. Defaults to miniflow.toml
        -e --env 'KEY=value'       Set an environment variable
        -h --help                  Show this help message

    FORMAT of miniflow.toml

        [env]
        ENV_VAR=value                # Environment variables that apply to the whole flow

        [steps.print-hello]
        cmd = 'echo hello'           # Command to run
        cwd = '.'                    # Working directory (optional, defaults to flow file directory)
        env = { 'ENV_VAR': 'value' } # Environment variables (optional)

        [steps.print-world]
        cmd = 'echo world'           # Command to run
        deps = ['print-hello']       # List of steps that this step depends on
        cwd = '.'                    # Working directory (optional, defaults to flow file directory)
        env = { 'ENV_VAR': 'value' } # Environment variables (optional)

    NOTES

        miniflow stores state information and logs in the _miniflow folder. Add _miniflow
        to your .gitignore.

