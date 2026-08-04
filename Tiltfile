# Tiltfile - drive maestro + a local Airflow from the Tilt UI.
#
# This is a Kubernetes-free Tiltfile: everything is a local_resource, so no
# Docker, cluster or registry is needed. Tilt is used purely as a control panel
# and file-watcher over the same steps scripts/maestro_airflow_up.sh runs.
#
#   tilt up                     # then press SPACE for the web UI
#   tilt down                   # stops Airflow and tears the resources down
#
# ---------------------------------------------------------------------------
# CONFIGURE ME
# Pass on the command line, e.g.
#   tilt up -- --project-dir ~/code/my_dbt_project --target dev --port 8080
# or set the matching env vars (DBT_PROJECT_DIR, DBT_PROFILES_DIR, ...).
# ---------------------------------------------------------------------------

config.define_string('project-dir')      # REQUIRED: your dbt project root
config.define_string('profiles-dir')     # dir containing profiles.yml
config.define_string('target')           # dbt target name
config.define_string('dbt-bin')          # dbt executable
config.define_string('port')             # Airflow UI port
config.define_string('airflow-version')  # Airflow to install
config.define_string('python-version')   # python for the Airflow venv
config.define_string('workdir')          # where generated state lives
config.define_string('dags-dir')         # where the generated DAG files land
config.define_string('min-models-per-dag')
config.define_string('orchestration')    # none | simple | staggered
config.define_string('default-command')  # build | run | test | run_test
cfg = config.parse()


def opt(name, env, default):
    return cfg.get(name, os.getenv(env, default))


def q(value):
    """Single-quote a value for the shell. Starlark has no shlex module."""
    return "'" + str(value).replace("'", "'\\''") + "'"


PROJECT_DIR = opt('project-dir', 'DBT_PROJECT_DIR', '')
PROFILES_DIR = opt('profiles-dir', 'DBT_PROFILES_DIR', os.getenv('HOME', '') + '/.dbt')
TARGET = opt('target', 'DBT_TARGET', 'dev')
DBT_BIN = opt('dbt-bin', 'DBT_BIN', 'dbt')
PORT = opt('port', 'PORT', '8080')
AIRFLOW_VERSION = opt('airflow-version', 'AIRFLOW_VERSION', '2.10.5')
PYTHON_VERSION = opt('python-version', 'PYTHON_VERSION', '3.12')
WORKDIR = opt('workdir', 'WORKDIR', os.getcwd() + '/.maestro-airflow')
# Left empty unless you actually asked for a path. An empty value means "don't
# pass --dags-dir at all", which lets the launcher apply its own precedence:
# airflow.dags_dir from a maestro config in the project/cwd first, and only then
# the ~/Desktop/maestro-dags fallback. Passing the flag unconditionally would
# override a project that already declares where its DAGs belong.
DAGS_DIR = opt('dags-dir', 'DAGS_DIR', '')
MIN_MODELS = opt('min-models-per-dag', 'MIN_MODELS_PER_DAG', '1')
ORCHESTRATION = opt('orchestration', 'ORCHESTRATION_MODE', 'none')
DEFAULT_COMMAND = opt('default-command', 'DEFAULT_COMMAND', 'build')

if not PROJECT_DIR:
    # Starlark has no implicit string concatenation - join with '+'.
    fail(
        "project-dir is required.\n" +
        "  tilt up -- --project-dir /path/to/your/dbt/project\n" +
        "or: export DBT_PROJECT_DIR=/path/to/your/dbt/project"
    )

UP = './scripts/maestro_airflow_up.sh'
_args = [
    '--project-dir', q(PROJECT_DIR),
    '--profiles-dir', q(PROFILES_DIR),
    '--target', q(TARGET),
    '--dbt-bin', q(DBT_BIN),
    '--port', q(PORT),
    '--workdir', q(WORKDIR),
    '--airflow-version', q(AIRFLOW_VERSION),
    '--python-version', q(PYTHON_VERSION),
    '--min-models-per-dag', q(MIN_MODELS),
    '--orchestration', q(ORCHESTRATION),
    '--default-command', q(DEFAULT_COMMAND),
]
# Only forward --dags-dir when it was actually set - see the DAGS_DIR comment.
if DAGS_DIR:
    _args = _args + ['--dags-dir', q(DAGS_DIR)]
COMMON = ' '.join(_args)

print('maestro/Airflow via Tilt')
print('  project  : ' + PROJECT_DIR)
print('  profiles : ' + PROFILES_DIR)
print('  target   : ' + TARGET)
print('  workdir  : ' + WORKDIR)
print('  dags dir : ' + (DAGS_DIR if DAGS_DIR else '(from maestro config, else ~/Desktop/maestro-dags)'))
print('  UI       : http://localhost:' + PORT)

# ---------------------------------------------------------------------------
# 1. unit tests - fast feedback while editing the generator
# ---------------------------------------------------------------------------
local_resource(
    'maestro-tests',
    cmd='.venv/bin/pytest tests/ -q 2>/dev/null || pytest tests/ -q',
    deps=['dbt_job_maestro', 'tests'],
    labels=['maestro'],
    allow_parallel=True,
)

# ---------------------------------------------------------------------------
# 2. generate selectors.yml + DAG files, and re-generate whenever the
#    generator or the project's models change
# ---------------------------------------------------------------------------
local_resource(
    'generate-dags',
    cmd=UP + ' regen ' + COMMON,
    deps=[
        'dbt_job_maestro',
        PROJECT_DIR + '/models',
        PROJECT_DIR + '/dbt_project.yml',
    ],
    labels=['maestro'],
    resource_deps=['maestro-tests'],
)

# ---------------------------------------------------------------------------
# 3. Airflow itself.
#
#    `cmd` prepares (install venv, generate, migrate the DB) and `serve_cmd`
#    runs `airflow standalone` in the FOREGROUND. Tilt must own the long-running
#    process: the `up` subcommand backgrounds Airflow and returns, and Tilt reaps
#    the command's process group on exit, which killed Airflow immediately.
#
#    `down` runs first so a Tiltfile edit or manual re-trigger restarts cleanly -
#    otherwise a second Airflow starts while the first still holds the web port
#    (and standalone's fixed internal ports 8793/8794) and both end up broken.
# ---------------------------------------------------------------------------
local_resource(
    'airflow',
    cmd=UP + ' down ' + COMMON + ' >/dev/null 2>&1; ' + UP + ' prepare ' + COMMON,
    serve_cmd=UP + ' serve ' + COMMON,
    readiness_probe=probe(
        period_secs=10,
        timeout_secs=5,
        # 127.0.0.1, not 'localhost': Tilt's Go dialer resolves localhost to IPv6
        # [::1] while Airflow's gunicorn binds IPv4 only, so the probe would fail
        # with "dial tcp [::1]:<port>: connect: connection refused" even though
        # the UI is serving fine.
        http_get=http_get_action(port=int(PORT), host='127.0.0.1', path='/health'),
    ),
    links=[link('http://localhost:' + PORT, 'Airflow UI')],
    labels=['airflow'],
    resource_deps=['generate-dags'],
)

# ---------------------------------------------------------------------------
# 4. manual buttons - triggered from the Tilt UI, never automatically
# ---------------------------------------------------------------------------
local_resource(
    'airflow-status',
    cmd=UP + ' status ' + COMMON,
    labels=['airflow'],
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False,
)

local_resource(
    'airflow-down',
    cmd=UP + ' down ' + COMMON,
    labels=['airflow'],
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False,
)

# Print where the credentials live once Airflow is up.
local_resource(
    'airflow-credentials',
    # printf, not `echo -n`: Tilt runs commands through `sh`, whose builtin echo
    # prints a literal "-n" on macOS.
    cmd=(
        'printf "user: admin\\npassword: "; ' +
        'cat ' + q(WORKDIR + '/airflow_home/standalone_admin_password.txt')
    ),
    labels=['airflow'],
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False,
)
