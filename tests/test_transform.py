import re
from unittest import TestCase

from json_pipeline.transform import Transform, Args
from json_pipeline.utils import dict_to_text


class TransformTest(TestCase):

    def test_filter_regex(self):
        """Filters out records that don't match given regex in the given field.
        Also accepts regex_flags.
        """
        args = Transform.args_from_dict({
            'operation': 'filter_regex',
            'field': 'name',
            'regex': r'office_\d+',
            'regex_flags': ['I'],
        })
        dataset = [{'name': 'Office_A'}, {'name': 'Office_1'}]
        self.assertEqual(list(Transform().run(dataset, args)), [{'name': 'Office_1'}])

    def test_filter_regex_neg(self):
        """Filters out records that match given regex in the given field
        Also accepts regex_flags.
        """
        args = Transform.args_from_dict({
            'operation': 'filter_regex_neg',
            'field': 'name',
            'regex': r'office_\d+',
            'regex_flags': ['I'],
        })
        dataset = [{'name': 'Office_A'}, {'name': 'Office_1'}]
        self.assertEqual(list(Transform().run(dataset, args)), [{'name': 'Office_A'}])

    def test_cross_filter(self):
        """Filters out records for which given field don't match value from another (target) field
        """
        args = Transform.args_from_dict({
            'operation': 'cross_filter',
            'field': 'name',
            'target': 'description',
        })
        dataset = [{'name': 'Office_A', 'description': 'Headquarter'}, {'name': 'Office_B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)), [{'name': 'Office_B', 'description': 'Office'}])

    def test_filter_not_exists(self):
        """Filters out records that doesn't have given field
        """
        args = Transform.args_from_dict({
            'operation': 'filter_not_exists',
            'field': 'description',
        })
        dataset = [{'name': 'Office_A', 'description': 'Headquarter'}, {'name': 'Office_B'}]
        self.assertEqual(list(Transform().run(dataset, args)), [{'name': 'Office_A', 'description': 'Headquarter'}])

    def test_rename_field(self):
        """Rename provided field to the target one.
        """
        args = Transform.args_from_dict({
            'operation': 'rename_field',
            'field': 'description',
            'target': 'title',
        })
        dataset = [{'name': 'Office_A', 'description': 'Headquarter'}, {'name': 'Office_B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                  [{'name': 'Office_A', 'title': 'Headquarter'}, {'name': 'Office_B', 'title': 'Office'}])

    def test_extract(self):
        """Extracts the regex group from the given field, and save in the given target field
        Also accepts regex_flags.
        """
        args = Transform.args_from_dict({
            'operation': 'extract',
            'field': 'name',
            'target': 'id',
            'regex': r'office_(\w+)',
            'regex_flags': ['I'],
        })
        dataset = [{'name': 'Office_A', 'description': 'Headquarter'}, {'name': 'Office_B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                  [{'name': 'Office_A', 'description': 'Headquarter', 'id': 'A'},
                   {'name': 'Office_B', 'description': 'Office', 'id': 'B'}])

    def test_template(self):
        """Copy given fields (in template format as per str.format() function) from each record in a dataset,
        into the given target field.
        """
        args = Transform.args_from_dict({
            'operation': 'template',
            'field': '{description}_{id}',
            'target': 'name',
        })
        dataset = [{'id': 'A', 'description': 'Office'}, {'id': 'B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                  [{'name': 'Office_A', 'description': 'Office', 'id': 'A'},
                   {'name': 'Office_B', 'description': 'Office', 'id': 'B'}])

    def test_remove_fields_comma_separated(self):
        """Remove the given fields (as string, comma-separated fields) of each record in a dataset.
        """
        args = Transform.args_from_dict({
            'operation': 'remove_fields',
            'field': 'description,id',
        })
        dataset = [{'name': 'Office_A', 'id': 'A', 'description': 'Office'},
                   {'name': 'Office_B', 'id': 'B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                  [{'name': 'Office_A'}, {'name': 'Office_B'}])

    def test_remove_fields_list(self):
        """Remove the given fields (as list) of each record in a dataset.
        """
        args = Transform.args_from_dict({
            'operation': 'remove_fields',
            'field': ['description', 'id'],
        })
        dataset = [{'name': 'Office_A', 'id': 'A', 'description': 'Office'},
                   {'name': 'Office_B', 'id': 'B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                  [{'name': 'Office_A'}, {'name': 'Office_B'}])

    
    def test_dedupe(self):
        """Dedupe using given field as deduping key, Only first appearance survives.
        """
        args = Transform.args_from_dict({
            'operation': 'dedupe',
            'field': 'id',
        })
        dataset = [{'name': 'Office_A', 'id': 'A', 'description': 'Office'},
                   {'name': 'Office_B', 'id': 'B', 'description': 'Office'},
                   {'name': 'Office_A', 'id': 'A', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                         [{'name': 'Office_A', 'id': 'A', 'description': 'Office'},
                          {'name': 'Office_B', 'id': 'B', 'description': 'Office'}])

    def test_preset(self):
        """Preset filtering pipeline. Pipeline must be a list of mapping from a keyword (provided as target)
        to a list of Args objects
        """
        class MyTransform(Transform):
            PIPELINE = {
                'usps.com': [
                    Transform.args_from_dict({'operation': 'dedupe', 'field': 'id',}),
                    Transform.args_from_dict({'operation': 'remove_fields', 'field': 'id',}),
                ],
            }

        args = MyTransform.args_from_dict({
            'operation': 'preset',
            'target': 'usps.com',
        })
        dataset = [{'name': 'Office_A', 'id': 'A', 'description': 'Office'},
                   {'name': 'Office_B', 'id': 'B', 'description': 'Office'},
                   {'name': 'Office_A', 'id': 'A', 'description': 'Office'}]
        self.assertEqual(list(MyTransform().run(dataset, args)),
                         [{'name': 'Office_A', 'description': 'Office'},
                          {'name': 'Office_B', 'description': 'Office'}])

    def test_plaintext(self):
        """
        Converts text to plain:
        - lowers letters
        - replace spaces and hyphens by _
        - remove any other character that are not digits
        """
        args = Transform.args_from_dict({
            'operation': 'plaintext',
            'field': 'id',
        })
        dataset = [{'name': 'Office A', 'id': 'Office A', 'description': 'Office'},
                   {'name': 'Office B', 'id': 'Office B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                         [{'name': 'Office A', 'id': 'office_a', 'description': 'Office'},
                          {'name': 'Office B', 'id': 'office_b', 'description': 'Office'}])

    def test_function(self):
        """
        Apply given function (as an absolute python path) to each record.
        Function parameters are a record and operation arguments object.
        Return value is the modified record.
        Function is provided in the field argument.
        """
        args = Transform.args_from_dict({
            'operation': 'function',
            'field': dict_to_text,
            'target': 'open_hours',
            'separator': ', ',
        })
        dataset = [{'name': 'Office A', 'open_hours': {'Monday': '9:00-18:00', 'Tuesday': '8:00-18:00'}},
                   {'name': 'Office B', 'open_hours': {'Monday-Friday': '8:00-20:00', 'Saturday': '10:00-18:00'}}]
        self.assertEqual(list(Transform().run(dataset, args)),
                   [{'name': 'Office A', 'open_hours': 'Monday: 9:00-18:00, Tuesday: 8:00-18:00'},
                    {'name': 'Office B', 'open_hours': 'Monday-Friday: 8:00-20:00, Saturday: 10:00-18:00'}])

    def test_fixed_value(self):
        """Function is provided in the field argument.
        """
        args = Transform.args_from_dict({
            'operation': 'fixedvalue',
            'field': 'chain',
            'target': 'USPS',
        })
        dataset = [{'name': 'Office A', 'description': 'Headquarter'},
                   {'name': 'Office B', 'description': 'Office'}]
        self.assertEqual(list(Transform().run(dataset, args)),
                         [{'name': 'Office A', 'chain': 'USPS', 'description': 'Headquarter'},
                          {'name': 'Office B', 'chain': 'USPS', 'description': 'Office'}])
