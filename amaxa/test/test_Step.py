import unittest
from unittest.mock import Mock, MagicMock, PropertyMock, patch
from .. import amaxa


class test_Step(unittest.TestCase):
    def test_scan_fields_identifies_self_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'Other__c': {
                'name': 'Other__c',
                'type': 'reference',
                'referenceTo': ['Contact']
            }
        })

        step = amaxa.Step('Account', ['Lookup__c', 'Other__c'])
        oc.add_step(step)

        step.scan_fields()

        self.assertEqual(set(['Lookup__c']), step.self_lookups)
    
    def test_scan_fields_identifies_dependent_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'Other__c': {
                'name': 'Other__c',
                'type': 'reference',
                'referenceTo': ['Contact']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])

        step = amaxa.Step('Account', ['Lookup__c', 'Other__c'])
        oc.add_step(step)

        step.scan_fields()

        self.assertEqual(set(['Other__c']), step.dependent_lookups)
    
    def test_scan_fields_identifies_all_lookups_within_extraction(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'Other__c': {
                'name': 'Other__c',
                'type': 'reference',
                'referenceTo': ['Contact']
            },
            'Outside__c': {
                'name': 'Outside__c',
                'type': 'reference',
                'referenceTo': ['Opportunity']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])

        step = amaxa.Step('Account', ['Lookup__c', 'Other__c', 'Outside__c'])
        oc.add_step(step)

        step.scan_fields()

        self.assertEqual(set(['Other__c', 'Lookup__c']), step.all_lookups)
        
    def test_scan_fields_identifies_descendent_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'Other__c': {
                'name': 'Other__c',
                'type': 'reference',
                'referenceTo': ['Contact']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])

        step = amaxa.Step('Contact', ['Lookup__c', 'Other__c'])
        oc.add_step(step)

        step.scan_fields()

        self.assertEqual(set(['Lookup__c']), step.descendent_lookups)
    
    def test_scan_fields_handles_mixed_polymorphic_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
        oc.get_field_map = Mock(return_value={
            'Poly_Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account', 'Opportunity']
            },
            'Other__c': {
                'name': 'Other__c',
                'type': 'reference',
                'referenceTo': ['Contact']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact', 'Opportunity'])

        step = amaxa.Step('Contact', ['Poly_Lookup__c', 'Other__c'])
        oc.add_step(step)

        step.scan_fields()

        self.assertEqual(set(['Poly_Lookup__c']), step.dependent_lookups)
        self.assertEqual(set(['Poly_Lookup__c']), step.descendent_lookups)

    def test_generates_field_list(self):
        step = amaxa.Step('Account', ['Lookup__c', 'Other__c'])

        self.assertEqual('Lookup__c, Other__c', step.get_field_list())