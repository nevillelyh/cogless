import importlib
import inspect
import os
import os.path
import re
from typing import Any, Dict, Iterator

import cog
from cog.internal import adt, util


class Runner:
    def __init__(self, predictor: adt.Predictor):
        module = importlib.import_module(predictor.module_name)
        cls = getattr(module, predictor.class_name)
        self.inputs = predictor.inputs
        self.output = predictor.output
        self.predictor = cls()

    def setup(self) -> None:
        if 'weights' in inspect.signature(self.predictor.setup).parameters:
            url = os.environ.get('COG_WEIGHTS')
            path = 'weights'
            if url:
                self.predictor.setup(weights=url)
            elif os.path.exists(path):
                self.predictor.setup(weights=cog.Path(path))
            else:
                self.predictor.setup(weights=None)
        else:
            self.predictor.setup()

    def is_iter(self) -> bool:
        return self.output.kind in {
            adt.Kind.ITERATOR,
            adt.Kind.CONCAT_ITERATOR,
        }

    def _kwargs(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        for name, value in inputs.items():
            adt_in = self.inputs[name]
            cog_t = adt_in.type
            if adt_in.is_list:
                assert all(
                    util.check_value(cog_t, v) for v in value
                ), f'incompatible input for: {name}'
                value = [util.normalize_value(cog_t, v) for v in value]
            else:
                assert util.check_value(cog_t, value), f'incompatible input for: {name}'
                value = util.normalize_value(cog_t, value)
            kwargs[name] = value
        for name, adt_in in self.inputs.items():
            if name not in kwargs:
                assert adt_in.default is not None, f'missing default value for: {name}'
                kwargs[name] = adt_in.default

            vals = kwargs[name] if adt_in.is_list else [kwargs[name]]
            if adt_in.ge is not None:
                assert (
                    x >= adt_in.ge for x in vals
                ), f'not all values >= {adt_in.ge} for: {name}'
            if adt_in.le is not None:
                assert (
                    x <= adt_in.le for x in vals
                ), f'not all values <= {adt_in.le} for: {name}'
            if adt_in.min_length is not None:
                assert (
                    len(x) >= adt_in.min_length for x in vals
                ), f'not all values have len(x) >= {adt_in.min_length} for: {name}'
            if adt_in.max_length is not None:
                assert (
                    len(x) <= adt_in.max_length for x in vals
                ), f'not all values have len(x) <= {adt_in.max_length} for: {name}'
            if adt_in.regex is not None:
                p = re.compile(adt_in.regex)
                assert all(
                    p.match(x) is not None for x in vals
                ), f'not all inputs match regex for: {name}'
            if adt_in.choices is not None:
                assert all(
                    x in adt_in.choices for x in vals
                ), f'not all inputs in choices for: {name}'
        return kwargs

    def _check_output(self, output: Any) -> Any:
        if self.output.kind is adt.Kind.SINGLE:
            assert self.output.type is not None, 'missing output type'
            assert util.check_value(
                self.output.type, output
            ), f'incompatible output: {output}'
            return output
        elif self.output.kind is adt.Kind.LIST:
            assert self.output.type is not None, 'missing output type'
            assert type(output) is list, 'output is not list'
            for x in output:
                assert util.check_value(
                    self.output.type, x
                ), f'incompatible output: {x}'
            return output
        elif self.output.kind == adt.Kind.OBJECT:
            assert self.output.fields is not None, 'missing output fields'
            for name, tpe in self.output.fields.items():
                assert hasattr(output, name), f'missing output field: {name}'
                value = getattr(output, name)
                assert util.check_value(
                    tpe, value
                ), f'incompatible output: {name}={value}'
            return output

    def predict(self, inputs: Dict[str, Any]) -> Any:
        assert not self.is_iter(), 'predict returns iterator, call predict_iter instead'
        output = self.predictor.predict(**self._kwargs(inputs))
        return self._check_output(output)

    def predict_iter(self, inputs: Dict[str, Any]) -> Iterator[Any]:
        assert self.is_iter(), 'predict does not return iterator, call predict instead'

        result = self.predictor.predict(**self._kwargs(inputs))
        assert self.output.type is not None, 'missing output type'
        for x in result:
            assert util.check_value(self.output.type, x), f'incompatible output: {x}'
            yield x
