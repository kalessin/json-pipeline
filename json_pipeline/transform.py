import re
import sys
import json
import argparse
from functools import partial
from collections import namedtuple

from .utils import load_object


_args_properties = ['operation', 'field', 'regex', 'target', 'separator', 'regex_flags', 'regex_per_item', 'preset', 'pipeline']
Args = namedtuple('OpArgs', _args_properties)


def reflags(lst):
    lst = lst or []
    result = 0
    for op in [getattr(re, f) for f in lst]:
        result = result | op
    return result


_UNDERSCORE_RE = re.compile(r'[\s-]+')
_REMOVE_RE = re.compile(r'[^\w\d\s-]+')


def plain(txt):
    txt = txt.lower()
    txt = _UNDERSCORE_RE.sub('_', txt)
    txt = _REMOVE_RE.sub('', txt)
    return txt


class Transform:

    OPERATIONS = [
        'filter_regex',
        'filter_regex_neg',
        'cross_filter',
        'filter_not_exists',
        'rename_field',
        'extract',
        'template',
        'remove_fields',
        'dedupe',
        'preset',
        'plaintext',
        'function',
        'fixed_value',
    ]
    DEFAULTS = {
        'regex_flags': list,
        'separator': str,
    }
    PIPELINE = None

    @staticmethod
    def filter_regex(dataset, args):
        """
        Filters out records that don't match given regex in the given field
        """
        regex_re = re.compile(args.regex, flags=reflags(args.regex_flags))
        for d in dataset:
            if args.field in d:
                if regex_re.search(d[args.field]):
                    yield d
            else:
                yield d

    @staticmethod
    def filter_regex_neg(dataset, args):
        """
        Filters out records that match given regex in the given field
        """
        regex_re = re.compile(args.regex, flags=re.I)
        for d in dataset:
            if args.field in d:
                if regex_re.search(d[args.field]) is None:
                    yield d
            else:
                yield d

    @staticmethod
    def cross_filter(dataset, args):
        """
        Filters out records for which given field don't match value from another (target) field
        """
        for d in dataset:
            if args.field in d and args.target in d:
                if d[args.target] in d[args.field]:
                    yield d
            else:
                yield d

    @staticmethod
    def filter_not_exists(dataset, args):
        """
        Filters out records that doesn't have given field
        """
        for d in dataset:
            if args.field in d:
                yield d

    @staticmethod
    def rename_field(dataset, args):
        """
        Rename provided field to the target one.
        """
        for d in dataset:
            if args.field in d:
                d[args.target] = d.pop(args.field)
            yield d

    @staticmethod
    def extract(dataset, args):
        """
        Extracts the regex group from the given field, and save in the given target field
        """
        if args.regex is not None:
            regex_re = re.compile(args.regex, flags=reflags(args.regex_flags))
        for d in dataset:
            if args.field in d:
                if args.regex_per_item is not None:
                    regex_re = re.compile(args.regex_per_item.format(**d), flags=reflags(args.regex_flags))
                m = regex_re.search(d[args.field])
                if m:
                    if m.groups():
                        d[args.target] = m.groups()[0]
                    else:
                        d[args.target] = m.group()
            yield d

    @staticmethod
    def template(dataset, args):
        """
        Copy given fields (in template format) from each record in a dataset,
        into the given target field
        """
        for d in dataset:
            d[args.target] = args.field.format(**d)
            yield d

    @staticmethod
    def remove_fields(dataset, args):
        """
        Remove the given fields (either comma-separated or a list) of each record in a dataset.
        """
        fields= args.field
        if isinstance(fields, str):
            fields = fields.split(',')
        for d in dataset:
            for field in fields:
                d.pop(field, None)
            yield d

    @staticmethod
    def dedupe(dataset, args):
        """
        Dedupe using given field as deduping key
        """
        seen = set()
        for d in dataset:
            if args.field not in d:
                yield d
            elif d[args.field] not in seen:
                seen.add(d[args.field])
                yield d

    @classmethod
    def preset(cls, dataset, args):
        """Preset filtering. Pipeline must be a mapping from a pipeline name (provided in args.target)
        to a list of Args objects. Pipeline can be given via arguments or as a class attribute.
        """
        pipeline = load_object(args.pipeline) if args.pipeline else cls.PIPELINE
        assert pipeline, 'A pipeline must be defined.'
        result = dataset
        for opargs in pipeline[args.target]:
            opname = opargs.operation
            op = getattr(cls, opname)
            result = op(result, opargs)
        return result

    @staticmethod
    def plaintext(dataset, args):
        """
        Converts text to plain:
        - lowers letters
        - replace spaces and hyphens by _
        - remove any other character that are not digits
        """
        for d in dataset:
            d[args.field] = plain(d[args.field])
            yield d

    @staticmethod
    def function(dataset, args):
        """
        Apply given function (if string, as an absolute python path) to each record.
        Function parameters are one record and command line args.
        Function return value is the modified record.
        Function is provided in the field argument.
        """
        func = args.field
        if isinstance(func, str):
            func = load_objec(func)
        for d in dataset:
            yield func(d, args)

    @staticmethod
    def fixedvalue(dataset, args):
        """
        Add a fixed target value to the given field in every record
        """
        for d in dataset:
            d[args.field] = args.target
            yield d

    def run(self, dataset, args):
        operation = partial(getattr(self, args.operation), args=args)
        for d in operation(dataset):
            yield d

    @classmethod
    def get_default(cls, option):
        factory = cls.DEFAULTS.get(option, lambda : None)
        return factory()

    @classmethod
    def args_from_dict(cls, jsonspec):
        jsonspec = jsonspec.copy()
        for prop in _args_properties:
            jsonspec.setdefault(prop, cls.get_default(prop))
        return Args(**jsonspec)


class TransformScript(Transform):

    def __init__(self):
        self.args = self.parse_args()

    def parse_args(self):
        self.argparser = argparse.ArgumentParser()
        self.add_argparser_options()
        args = self.argparser.parse_args()
        return args

    def add_argparser_options(self):
        self.argparser.add_argument('operation', choices=Transform.OPERATIONS)
        self.argparser.add_argument('--field', help='target field')
        self.argparser.add_argument('--regex', help='Regex pattern')
        self.argparser.add_argument('--regex-flags', action='append', default=Transform.get_default('regex_flags'))
        self.argparser.add_argument('--target', help='Operation target (depends on operation)')
        self.argparser.add_argument('--separator', help='Defines separator in join operations',
                            default=Transform.get_default('separator'))
        self.argparser.add_argument('--regex-per-item', help='Regex pattern applied per item')
        self.argparser.add_argument('--pipeline', help='For preset operation, give pipeline \
                                                absolute python path object. It must be a dict')

        self.argparser.add_argument('--input', type=argparse.FileType('r'), help='Target file', default=sys.stdin)
        self.argparser.add_argument('--output', type=argparse.FileType('w'), help='Target file', default=sys.stdout)
       
    def main():
        dataset = [json.loads(l) for l in self.args.input]
        for d in Transform().run(dataset, self.args):
            print(json.dumps(d), file=self.args.output)
