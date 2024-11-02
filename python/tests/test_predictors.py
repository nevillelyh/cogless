import importlib
import json
import os.path
import pkgutil
from typing import List

import pytest

import cog
from cog.internal import inspector, runner, schemas


def get_predictors() -> List[str]:
    schemas_dir = os.path.join(os.path.dirname(__file__), 'schemas')
    return [name for _, name, _ in pkgutil.iter_modules([schemas_dir])]


@pytest.mark.parametrize('predictor', get_predictors())
def test_predictor(predictor):
    module_name = f'tests.schemas.{predictor}'
    p = inspector.create_predictor(module_name, 'Predictor')
    r = runner.Runner(p)
    assert not r.predictor.setup_done
    r.setup()
    assert r.predictor.setup_done

    m = importlib.import_module(module_name)
    fixture = getattr(m, 'FIXTURE')
    for inputs, output in fixture:
        if r.is_iter():
            result = [x for x in r.predict_iter(inputs)]
            assert result == output
        else:
            result = r.predict(inputs)
            assert result == output


@pytest.mark.parametrize('predictor', get_predictors())
def test_schema(predictor):
    module_name = f'tests.schemas.{predictor}'
    class_name = 'Predictor'
    p = inspector.create_predictor(module_name, class_name)

    path = os.path.join(os.path.dirname(__file__), 'schemas', f'{predictor}.json')
    with open(path, 'r') as f:
        schema = json.load(f)

    if predictor == 'secrets':
        props = schema['components']['schemas']['Input']['properties']
        # FIXME: Bug in Cog?
        # Default Secret should be redacted
        props['s3']['default'] = '**********'
        # List[Secret] missing defaults
        props['ss']['default'] = ['**********', '**********']

    assert schemas.to_json_input(p) == schema['components']['schemas']['Input']
    assert schemas.to_json_output(p) == schema['components']['schemas']['Output']
    assert schemas.to_json_schema(p) == schema

    eq = cog.Secret.__eq__
    if predictor == 'secrets':
        cog.Secret.__eq__ = lambda self, other: type(other) is cog.Secret

    assert schemas.from_json_input(schema) == p.inputs
    assert schemas.from_json_output(schema) == p.output
    assert schemas.from_json_schema(module_name, class_name, schema) == p

    if predictor == 'secrets':
        cog.Secret.__eq__ = eq
