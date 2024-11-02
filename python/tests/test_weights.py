import os.path

from cog.internal import inspector, runner


def test_weights_none():
    p = inspector.create_predictor('tests.schemas.weights', 'Predictor')
    r = runner.Runner(p)
    r.setup()
    assert r.predict({'i': 0}) == ''


def test_weights_url():
    p = inspector.create_predictor('tests.schemas.weights', 'Predictor')
    r = runner.Runner(p)
    os.environ['COG_WEIGHTS'] = 'http://r8.im/weights.tar'
    r.setup()
    assert r.predict({'i': 0}) == 'http://r8.im/weights.tar'
    del os.environ['COG_WEIGHTS']


def test_weights_path(tmp_path):
    p = inspector.create_predictor('tests.schemas.weights', 'Predictor')
    r = runner.Runner(p)
    os.chdir(tmp_path)
    os.mkdir(os.path.join(tmp_path, 'weights'))
    r.setup()
    assert r.predict({'i': 0}) == 'weights'
