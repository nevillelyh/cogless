import json
import os.path
import pathlib
import signal
import subprocess
import time
from typing import List

from cog.internal.file_runner import FileRunner


def setup_signals() -> List[int]:
    signals = []

    def handler(signum, _):
        signals.append(signum)

    signal.signal(signal.SIGUSR1, handler)
    signal.signal(signal.SIGUSR2, handler)
    return signals


def cmd(tmp_path: str) -> List[str]:
    return [
        'python3',
        '-m',
        'cog.internal.file_runner',
        '--working-dir',
        tmp_path,
        '--module-name',
        'tests.runners.sleep',
        '--class-name',
        'Predictor',
    ]


def test_file_runner(tmp_path):
    signals = setup_signals()

    env = os.environ.copy()
    env['SETUP_SLEEP'] = '1'
    p = subprocess.Popen(cmd(tmp_path), env=env)

    time.sleep(0.1)
    openapi_file = os.path.join(tmp_path, 'openapi.json')
    assert os.path.exists(openapi_file)

    setup_result_file = os.path.join(tmp_path, 'setup_result.json')
    assert not os.path.exists(setup_result_file)
    time.sleep(1.1)
    assert os.path.exists(setup_result_file)
    with open(setup_result_file) as f:
        setup_result = json.load(f)
    assert setup_result['status'] == 'succeeded'
    assert signals == [FileRunner.SIG_READY]

    req_file = os.path.join(tmp_path, 'request-a.json')
    resp_file = os.path.join(tmp_path, 'response-a.json')
    with open(req_file, 'w') as f:
        json.dump({'input': {'i': 1, 's': 'bar'}}, f)
    assert os.path.exists(req_file)
    assert not os.path.exists(resp_file)
    time.sleep(0.1)
    assert signals == [FileRunner.SIG_READY, FileRunner.SIG_BUSY]
    time.sleep(1.0)
    assert not os.path.exists(req_file)
    assert os.path.exists(resp_file)
    assert signals == [FileRunner.SIG_READY, FileRunner.SIG_BUSY, FileRunner.SIG_READY]

    with open(resp_file, 'r') as f:
        resp = json.load(f)
    assert resp['status'] == 'succeeded'
    assert resp['output'] == '*bar*'

    stop_file = os.path.join(tmp_path, 'stop')
    pathlib.Path(stop_file).touch()
    assert p.poll() is None
    time.sleep(0.1)
    assert p.poll() == 0


def test_file_runner_setup_failed(tmp_path):
    signals = setup_signals()

    env = os.environ.copy()
    env['SETUP_SLEEP'] = '1'
    env['SETUP_FAILURE'] = '1'
    p = subprocess.Popen(cmd(tmp_path), env=env)

    time.sleep(0.1)
    openapi_file = os.path.join(tmp_path, 'openapi.json')
    assert os.path.exists(openapi_file)

    setup_result_file = os.path.join(tmp_path, 'setup_result.json')
    assert not os.path.exists(setup_result_file)
    time.sleep(1.1)
    assert os.path.exists(setup_result_file)
    with open(setup_result_file) as f:
        setup_result = json.load(f)
    assert setup_result['status'] == 'failed'
    assert p.poll() == 1
    assert signals == []


def test_file_runner_predict_failed(tmp_path):
    signals = setup_signals()

    env = os.environ.copy()
    env['PREDICTION_FAILURE'] = '1'
    p = subprocess.Popen(cmd(tmp_path), env=env)

    time.sleep(0.1)
    openapi_file = os.path.join(tmp_path, 'openapi.json')
    assert os.path.exists(openapi_file)

    setup_result_file = os.path.join(tmp_path, 'setup_result.json')
    assert os.path.exists(setup_result_file)
    with open(setup_result_file) as f:
        setup_result = json.load(f)
    assert setup_result['status'] == 'succeeded'
    assert signals == [FileRunner.SIG_READY]

    req_file = os.path.join(tmp_path, 'request-a.json')
    resp_file = os.path.join(tmp_path, 'response-a.json')
    with open(req_file, 'w') as f:
        json.dump({'input': {'i': 1, 's': 'bar'}}, f)
    assert os.path.exists(req_file)
    assert not os.path.exists(resp_file)
    time.sleep(0.1)
    assert signals == [FileRunner.SIG_READY, FileRunner.SIG_BUSY]
    time.sleep(1.0)
    assert not os.path.exists(req_file)
    assert os.path.exists(resp_file)
    assert signals == [FileRunner.SIG_READY, FileRunner.SIG_BUSY, FileRunner.SIG_READY]

    with open(resp_file, 'r') as f:
        resp = json.load(f)
    assert resp['status'] == 'failed'
    assert resp['error'] == 'prediction failed'

    stop_file = os.path.join(tmp_path, 'stop')
    pathlib.Path(stop_file).touch()
    assert p.poll() is None
    time.sleep(0.1)
    assert p.poll() == 0
