import argparse
import json
import logging
import os
import re
import signal
import sys
import time
from typing import Any, Dict

from cog.internal import inspector, runner, schemas, util


class FileRunner:
    REQUEST_RE = re.compile(r'^request-(?P<pid>\S+).json$')
    RESPONSE_FMT = 'response-{pid}.json'

    SIG_READY = signal.SIGUSR1
    SIG_BUSY = signal.SIGUSR2

    def __init__(self, working_dir: str, module_name: str, class_name: str):
        self.working_dir = working_dir
        self.module_name = module_name
        self.class_name = class_name

    def start(self) -> int:
        logging.info(
            'starting file runner: working_dir=%s, predict=%s.%s',
            self.working_dir,
            self.module_name,
            self.class_name,
        )

        os.makedirs(self.working_dir, exist_ok=True)
        setup_result_file = os.path.join(self.working_dir, 'setup_result.json')
        stop_file = os.path.join(self.working_dir, 'stop')
        openapi_file = os.path.join(self.working_dir, 'openapi.json')
        if os.path.exists(setup_result_file):
            os.unlink(setup_result_file)
        if os.path.exists(stop_file):
            os.unlink(stop_file)
        if os.path.exists(openapi_file):
            os.unlink(openapi_file)

        logging.info('setup started')
        setup_result: Dict[str, Any] = {'started_at': util.now_iso()}
        try:
            p = inspector.create_predictor(self.module_name, self.class_name)
            with open(openapi_file, 'w') as f:
                schema = schemas.to_json_schema(p)
                json.dump(schema, f)
            r = runner.Runner(p)

            r.setup()
            logging.info('setup completed')
            setup_result['status'] = 'succeeded'
        except Exception as e:
            logging.error('setup failed: %s', e)
            setup_result['status'] = 'failed'
        finally:
            setup_result['completed_at'] = util.now_iso()
        with open(setup_result_file, 'w') as f:
            json.dump(setup_result, f)
        if setup_result['status'] == 'failed':
            return 1

        os.kill(os.getppid(), FileRunner.SIG_READY)
        while True:
            n = 0
            for entry in os.listdir(self.working_dir):
                if os.path.exists(stop_file):
                    logging.info('stopping file runner')
                    return 0

                m = self.REQUEST_RE.match(entry)
                if m is None:
                    continue

                os.kill(os.getppid(), FileRunner.SIG_BUSY)
                pid = m.group('pid')
                req_path = os.path.join(self.working_dir, entry)
                with open(req_path, 'r') as f:
                    request = json.load(f)

                logging.info('prediction started: id=%s', pid)
                response: Dict[str, Any] = {'started_at': util.now_iso()}
                try:
                    response['output'] = r.predict(request['input'])
                    logging.info('prediction completed: id=%s', pid)
                    response['status'] = 'succeeded'
                except Exception as e:
                    logging.error('prediction failed: id=%s %s', pid, e)
                    response['status'] = 'failed'
                    response['error'] = str(e)
                finally:
                    setup_result['completed_at'] = util.now_iso()
                os.unlink(req_path)

                resp_path = os.path.join(
                    self.working_dir, self.RESPONSE_FMT.format(pid=pid)
                )
                with open(resp_path, 'w') as f:
                    json.dump(response, f)
                os.kill(os.getppid(), FileRunner.SIG_READY)
                n += 1
            if n == 0:
                time.sleep(0.1)


parser = argparse.ArgumentParser()
parser.add_argument(
    '--working-dir', metavar='DIR', required=True, help='working directory'
)
parser.add_argument(
    '--module-name', metavar='NAME', required=True, help='Python module name'
)
parser.add_argument(
    '--class-name', metavar='NAME', required=True, help='Python class name'
)

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s\t%(levelname)s\t%(filename)s:%(lineno)d\t[COG] %(message)s'
        )
    )
    logger.addHandler(handler)
    args = parser.parse_args()
    fr = FileRunner(args.working_dir, args.module_name, args.class_name)
    sys.exit(fr.start())
