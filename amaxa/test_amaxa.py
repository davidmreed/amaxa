import unittest
import simple_salesforce
from unittest.mock import Mock, PropertyMock, patch
from . import amaxa
from . import transforms

class test_SalesforceId(unittest.TestCase):
    def test_converts_real_id_pairs(self):
        known_good_ids = {
            '01Q36000000RXX5': '01Q36000000RXX5EAO',
            '005360000016xkG': '005360000016xkGAAQ',
            '01I36000002zD9R': '01I36000002zD9REAU',
            '0013600001ohPTp': '0013600001ohPTpAAM',
            '0033600001gyv5B': '0033600001gyv5BAAQ'
        }

        for id_15 in known_good_ids:
            self.assertEqual(known_good_ids[id_15], str(amaxa.SalesforceId(id_15)))
            self.assertEqual(known_good_ids[id_15], amaxa.SalesforceId(id_15))
            self.assertEqual(amaxa.SalesforceId(id_15), known_good_ids[id_15])

            self.assertEqual(id_15, amaxa.SalesforceId(id_15))
            self.assertNotEqual(id_15, str(amaxa.SalesforceId(id_15)))

            self.assertEqual(amaxa.SalesforceId(id_15), amaxa.SalesforceId(id_15))
            self.assertEqual(amaxa.SalesforceId(str(amaxa.SalesforceId(id_15))), amaxa.SalesforceId(str(amaxa.SalesforceId(id_15))))

            self.assertEqual(known_good_ids[id_15], amaxa.SalesforceId(known_good_ids[id_15]))
            self.assertEqual(known_good_ids[id_15], str(amaxa.SalesforceId(known_good_ids[id_15])))

            self.assertEqual(hash(known_good_ids[id_15]), hash(amaxa.SalesforceId(id_15)))

    def test_raises_valueerror(self):
        with self.assertRaises(ValueError):
            # pylint: disable=W0612
            bad_id = amaxa.SalesforceId('test')

    def test_equals_other_id(self):
        the_id = amaxa.SalesforceId('001000000000000')

        self.assertEqual(the_id, amaxa.SalesforceId(the_id))

    def test_does_not_equal_other_value(self):
        the_id = amaxa.SalesforceId('001000000000000')

        self.assertNotEqual(the_id, 1)

    def test_str_repr_equal_18_char_id(self):
        the_id = amaxa.SalesforceId('001000000000000')

        self.assertEqual(the_id.id, str(the_id))
        self.assertEqual(the_id.id, repr(the_id))

    def test_hashing(self):
        id_set = set()
        for i in range(400):
            new_id = amaxa.SalesforceId('001000000000' + str(i + 1).zfill(3))
            self.assertNotIn(new_id, id_set)
            id_set.add(new_id)
            self.assertIn(new_id, id_set)

class test_Operation(unittest.TestCase):
    def test_stores_steps(self):
        connection = Mock()
        oc = amaxa.Operation(connection)

        step = Mock()
        oc.add_step(step)

        self.assertEqual([step], oc.steps)
        self.assertEqual(oc, step.context)
    
    def test_creates_and_caches_proxy_objects(self):
        connection = Mock()
        p = PropertyMock(return_value='Account')
        type(connection).Account = p

        oc = amaxa.Operation(connection)

        proxy = oc.get_proxy_object('Account')

        self.assertEqual('Account', proxy)
        p.assert_called_once_with()

        p.reset_mock()
        proxy = oc.get_proxy_object('Account')

        # Proxy should be cached
        self.assertEqual('Account', proxy)
        p.assert_not_called()

    def test_creates_and_caches_bulk_proxy_objects(self):
        connection = Mock()
        p = PropertyMock(return_value='Account')
        type(connection.bulk).Account = p

        oc = amaxa.Operation(connection)

        proxy = oc.get_bulk_proxy_object('Account')

        self.assertEqual('Account', proxy)
        p.assert_called_once_with()

        p.reset_mock()
        proxy = oc.get_bulk_proxy_object('Account')

        # Proxy should be cached
        self.assertEqual('Account', proxy)
        p.assert_not_called()

    @patch('amaxa.Operation.get_proxy_object')
    def test_caches_describe_results(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.Operation(connection)

        retval = oc.get_describe('Account')
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_describe('Account')
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_not_called()

    @patch('amaxa.Operation.get_proxy_object')
    def test_caches_field_maps(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.Operation(connection)

        retval = oc.get_field_map('Account')
        self.assertEqual({ 'Name': { 'name': 'Name' }, 'Id': { 'name': 'Id' } }, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_field_map('Account')
        self.assertEqual({ 'Name': { 'name': 'Name' }, 'Id': { 'name': 'Id' } }, retval)
        account_mock.describe.assert_not_called()

    @patch('amaxa.Operation.get_proxy_object')
    def test_filters_field_maps(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.Operation(connection)

        retval = oc.get_filtered_field_map('Account', lambda f: f['name'] == 'Id')
        self.assertEqual({ 'Id': { 'name': 'Id' } }, retval)

    def test_maps_ids_to_sobject_types(self):
        connection = Mock()
        connection.describe = Mock(return_value={
            'sobjects': {
                'Account': {
                    'keyPrefix': '001'
                },
                'Contact': {
                    'keyPrefix': '003'
                }
            }
        })

        oc = amaxa.Operation(connection)

        self.assertEqual('Account', oc.get_sobject_name_for_id('001000000000000'))
        self.assertEqual('Contact', oc.get_sobject_name_for_id('003000000000000'))

        connection.describe.assert_called_once_with()

class test_ExtractOperation(unittest.TestCase):
    def test_runs_all_steps(self):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)

        # pylint: disable=W0612
        for i in range(3):
            oc.add_step(Mock())
        
        oc.execute()

        for s in oc.steps:
            s.execute.assert_called_once_with()
            self.assertEqual(oc, s.context)
    
    def test_tracks_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_dependencies('Account'))

    def test_doesnt_add_dependency_for_extracted_record(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set(), oc.get_dependencies('Account'))

    def test_store_result_retains_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.extracted_ids['Account'])

    def test_store_result_writes_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })

    def test_store_result_transforms_output(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock
        mapper_mock = Mock()
        mapper_mock.transform_record = Mock(return_value = { 'Id': '001000000000000', 'Name': 'Caprica City Steel' })

        oc.mappers['Account'] = mapper_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        mapper_mock.transform_record.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica City Steel' })

    def test_store_result_clears_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set(), oc.get_dependencies('Account'))

    def test_store_result_does_not_write_duplicate_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.reset_mock()
        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_not_called()

    def test_get_extracted_ids_returns_results(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_extracted_ids('Account'))

    def test_get_sobject_ids_for_reference_returns_correct_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
        oc.get_field_map = Mock(return_value={ 'Lookup__c': { 'referenceTo': ['Account', 'Contact'] }})

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'University of Caprica' })
        oc.store_result('Contact', { 'Id': '003000000000000', 'Name': 'Gaius Baltar' })
        oc.store_result('Opportunity', { 'Id': '006000000000000', 'Name': 'Defense Mainframe' })

        self.assertEqual(set([amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('003000000000000')]),
                         oc.get_sobject_ids_for_reference('Account', 'Lookup__c'))


class test_DataMapper(unittest.TestCase):
    def test_transform_key_applies_mapping(self):
        mapper = amaxa.DataMapper({ 'Test': 'Value' })

        self.assertEqual('Value', mapper.transform_key('Test'))
        self.assertEqual('Foo', mapper.transform_key('Foo'))

    def test_transform_value_applies_transformations(self):
        mapper = amaxa.DataMapper({}, { 'Test__c': [transforms.strip, transforms.lowercase] })

        self.assertEqual('value', mapper.transform_value('Test__c', ' VALUE  '))

    def test_transform_record_does(self):
        mapper = amaxa.DataMapper(
            { 'Test__c': 'Value' },
            { 'Test__c': [transforms.strip, transforms.lowercase] }
        )

        self.assertEqual(
            { 'Value': 'nothing much', 'Second Key': 'another Response' },
            mapper.transform_record(
                { 'Test__c': '  NOTHING MUCH', 'Second Key': 'another Response' }
            )
        )

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


class test_ExtractionStep(unittest.TestCase):
    def retains_lookup_behavior_for_fields(self):
        step = amaxa.ExtractionStep(
            'Account',
            amaxa.ExtractionScope.ALL_RECORDS,
            ['Self_Lookup__c', 'Other__c'],
            amaxa.SelfLookupBehavior.TRACE_NONE,
            amaxa.OutsideLookupBehavior.INCLUDE
        )

        self.assertEqual(amaxa.SelfLookupBehavior.TRACE_NONE, step.get_self_lookup_behavior_for_field('Self_Lookup__c'))
        step.set_lookup_behavior_for_field('Self_Lookup__c', amaxa.SelfLookupBehavior.TRACE_ALL)
        self.assertEqual(amaxa.SelfLookupBehavior.TRACE_ALL, step.get_self_lookup_behavior_for_field('Self_Lookup__c'))
        self.assertEqual(amaxa.OutsideLookupBehavior.INCLUDE, step.get_outside_lookup_behavior_for_field('Other__c'))
        step.set_lookup_behavior_for_field('Other__c', amaxa.OutsideLookupBehavior.DROP_FIELD)
        self.assertEqual(amaxa.OutsideLookupBehavior.DROP_FIELD, step.get_outside_lookup_behavior_for_field('Other__c'))
    
    def test_store_result_calls_context(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account'])

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, [])
        oc.add_step(step)
        step.scan_fields()

        step.store_result({ 'Id': '001000000000000', 'Name': 'Picon Fleet Headquarters' })
        oc.store_result.assert_called_once_with('Account', { 'Id': '001000000000000', 'Name': 'Picon Fleet Headquarters' })
        oc.add_dependency.assert_not_called()

    def test_store_result_registers_self_lookup_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account'])

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        oc.add_step(step)
        step.scan_fields()

        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '001000000000001', 'Name': 'Picon Fleet Headquarters' })
        oc.add_dependency.assert_called_once_with('Account', amaxa.SalesforceId('001000000000001'))

    def test_store_result_respects_self_lookup_options(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account'])

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], None, amaxa.SelfLookupBehavior.TRACE_NONE)
        oc.add_step(step)
        step.scan_fields()

        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '001000000000001', 'Name': 'Picon Fleet Headquarters' })
        oc.add_dependency.assert_not_called()

    def test_store_result_registers_dependent_lookup_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Opportunity']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Opportunity'])

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        oc.add_step(step)
        step.scan_fields()

        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '006000000000001', 'Name': 'Picon Fleet Headquarters' })
        oc.add_dependency.assert_called_once_with('Opportunity', amaxa.SalesforceId('006000000000001'))

    def test_store_result_handles_polymorphic_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Opportunity', 'Account', 'Task']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact', 'Opportunity'])
        oc.get_extracted_ids = Mock(return_value=['001000000000001'])
        oc.get_sobject_name_for_id = Mock(side_effect=lambda id: {'001': 'Account', '006': 'Opportunity', '00T': 'Task'}[id[:3]])

        step = amaxa.ExtractionStep('Contact', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        oc.add_step(step)
        step.scan_fields()

        # Validate that the polymorphic lookup is treated properly when the content is a dependent reference
        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '006000000000001', 'Name': 'Kara Thrace' })
        oc.add_dependency.assert_called_once_with('Opportunity', amaxa.SalesforceId('006000000000001'))
        oc.store_result.assert_called_once_with('Contact', { 'Id': '001000000000000', 'Lookup__c': '006000000000001', 'Name': 'Kara Thrace' })
        oc.add_dependency.reset_mock()
        oc.store_result.reset_mock()

        # Validate that the polymorphic lookup is treated properly when the content is a descendent reference
        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '001000000000001', 'Name': 'Kara Thrace' })
        oc.add_dependency.assert_not_called()
        oc.store_result.assert_called_once_with('Contact', { 'Id': '001000000000000', 'Lookup__c': '001000000000001', 'Name': 'Kara Thrace' })
        oc.add_dependency.reset_mock()
        oc.store_result.reset_mock()

        # Validate that the polymorphic lookup is treated properly when the content is a off-extraction reference
        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '00T000000000001', 'Name': 'Kara Thrace' })
        oc.add_dependency.assert_not_called()
        oc.store_result.assert_called_once_with('Contact', { 'Id': '001000000000000', 'Lookup__c': '00T000000000001', 'Name': 'Kara Thrace' })

    def test_store_result_respects_outside_lookup_behavior_drop_field(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'AccountId': {
                'name': 'AccountId',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'LastName': {
                'name': 'Name',
                'type': 'string'
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            'Contact',
            amaxa.ExtractionScope.DESCENDENTS,
            ['AccountId'],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.DROP_FIELD
        )

        oc.add_step(step)
        step.scan_fields()

        step.store_result({'Id': '003000000000001', 'AccountId': '001000000000001'})
        oc.store_result.assert_called_once_with('Contact', {'Id': '003000000000001'})

    def test_store_result_respects_outside_lookup_behavior_error(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'AccountId': {
                'name': 'AccountId',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'LastName': {
                'name': 'Name',
                'type': 'string'
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            'Contact',
            amaxa.ExtractionScope.DESCENDENTS,
            ['AccountId'],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.ERROR
        )

        oc.add_step(step)
        step.scan_fields()

        with self.assertRaises(Exception):
            step.store_result({'Id': '003000000000001', 'AccountId': '001000000000001'})

    def test_store_result_respects_outside_lookup_behavior_include(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'AccountId': {
                'name': 'AccountId',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'LastName': {
                'name': 'Name',
                'type': 'string'
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            'Contact',
            amaxa.ExtractionScope.DESCENDENTS,
            ['AccountId'],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.INCLUDE
        )

        oc.add_step(step)
        step.scan_fields()

        step.store_result({'Id': '003000000000001', 'AccountId': '001000000000001'})
        oc.store_result.assert_called_once_with('Contact', {'Id': '003000000000001', 'AccountId': '001000000000001'})

    def test_store_result_discriminates_polymorphic_lookup_type(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'AccountId': {
                'name': 'AccountId',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'WhoId': {
                'name': 'Name',
                'type': 'string'
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact', 'Task'])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            'Contact',
            amaxa.ExtractionScope.DESCENDENTS,
            ['AccountId'],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.DROP_FIELD
        )

        oc.add_step(step)
        step.scan_fields()

        step.store_result({'Id': '003000000000001', 'AccountId': '001000000000001'})
        oc.store_result.assert_called_once_with('Contact', {'Id': '003000000000001'})

    def test_perform_lookup_pass_executes_correct_query(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_sobject_ids_for_reference = Mock(return_value=set([amaxa.SalesforceId('001000000000000')]))

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        oc.add_step(step)
        step.scan_fields()

        step.perform_id_field_pass = Mock()
        step.perform_lookup_pass('Lookup__c')

        oc.get_sobject_ids_for_reference.assert_called_once_with('Account', 'Lookup__c')
        step.perform_id_field_pass.assert_called_once_with('Lookup__c', set([amaxa.SalesforceId('001000000000000')]))

    def test_perform_id_field_pass_queries_all_records(self):
        connection = Mock()
        connection.query_all = Mock(side_effect=lambda x: { 'records': [{ 'Id': '001000000000001'}] })

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        step.store_result = Mock()
        oc.add_step(step)
        step.scan_fields()

        id_set = set()
        # Generate enough fake Ids to require two queries.
        for i in range(400):
            new_id = amaxa.SalesforceId('001000000000' + str(i + 1).zfill(3))
            id_set.add(new_id)

        self.assertEqual(400, len(id_set))

        step.perform_id_field_pass('Lookup__c', id_set)

        self.assertLess(1, len(connection.query_all.call_args_list))
        total = 0
        for call in connection.query_all.call_args_list:
            self.assertLess(len(call[0][0]) - call[0][0].find('WHERE'), 4000)
            total += call[0][0].count('\'001')
        self.assertEqual(400, total)

    def test_perform_id_field_pass_stores_results(self):
        connection = Mock()
        connection.query_all = Mock(side_effect=lambda x: { 'records': [{ 'Id': '001000000000001'}, { 'Id': '001000000000002'}] })

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        step.store_result = Mock()
        oc.add_step(step)
        step.scan_fields()

        step.perform_id_field_pass('Lookup__c', set([amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000002')]))
        step.store_result.assert_any_call(connection.query_all('Account')['records'][0])
        step.store_result.assert_any_call(connection.query_all('Account')['records'][1])

    def test_perform_id_field_pass_ignores_empty_set(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        oc.add_step(step)
        step.scan_fields()

        step.perform_id_field_pass('Lookup__c', set())

        connection.query_all.assert_not_called()

    def test_perform_bulk_api_pass_performs_query(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        bulk_proxy = Mock()
        bulk_proxy.query = Mock(side_effect=lambda x: { 'records': [{ 'Id': '001000000000001'}, { 'Id': '001000000000002'}] })
        oc.get_bulk_proxy_object = Mock(return_value=bulk_proxy)

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.QUERY, ['Lookup__c'])
        step.store_result = Mock()
        oc.add_step(step)
        step.scan_fields()

        step.perform_bulk_api_pass('SELECT Id FROM Account')
        oc.get_bulk_proxy_object.assert_called_once_with('Account')
        bulk_proxy.query.assert_called_once_with('SELECT Id FROM Account')

    def test_perform_bulk_api_pass_stores_results(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        bulk_proxy = Mock()
        bulk_proxy.query = Mock(return_value=[{ 'Id': '001000000000001'}, { 'Id': '001000000000002'}])
        oc.get_bulk_proxy_object = Mock(return_value=bulk_proxy)

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        step.store_result = Mock()
        oc.add_step(step)
        step.scan_fields()

        step.perform_bulk_api_pass('SELECT Id FROM Account')
        step.store_result.assert_any_call(bulk_proxy.query.return_value[0])
        step.store_result.assert_any_call(bulk_proxy.query.return_value[1])

    def test_resolve_registered_dependencies_loads_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_dependencies = Mock(
            side_effect=[
                set([
                    amaxa.SalesforceId('001000000000001'),
                    amaxa.SalesforceId('001000000000002')
                ]),
                set()
            ]
        )

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        step.perform_id_field_pass = Mock()
        oc.add_step(step)
        step.scan_fields()

        step.resolve_registered_dependencies()

        oc.get_dependencies.assert_has_calls([unittest.mock.call('Account'), unittest.mock.call('Account')])
        step.perform_id_field_pass.assert_called_once_with('Id', set([amaxa.SalesforceId('001000000000001'),
            amaxa.SalesforceId('001000000000002')]))

    def test_resolve_registered_dependencies_throws_exception_for_missing_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_dependencies = Mock(
            return_value=[
                set([
                    amaxa.SalesforceId('001000000000001'),
                    amaxa.SalesforceId('001000000000002')
                ]),
                set([
                    amaxa.SalesforceId('001000000000002')
                ])
            ]
        )

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'])
        step.perform_id_field_pass = Mock()
        oc.add_step(step)
        step.scan_fields()

        with self.assertRaises(Exception):
            step.resolve_registered_dependencies()

    def test_execute_with_all_records_performs_bulk_api_pass(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            }
        })

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Name'])
        step.perform_bulk_api_pass = Mock()
        oc.add_step(step)

        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with('SELECT Name FROM Account')

    def test_execute_with_query_performs_bulk_api_pass(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            }
        })

        step = amaxa.ExtractionStep('Account', amaxa.ExtractionScope.QUERY, ['Name'], 'Name != null')
        step.perform_bulk_api_pass = Mock()
        oc.add_step(step)

        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with('SELECT Name FROM Account WHERE Name != null')

    def test_execute_loads_all_descendents(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            },
            'AccountId': {
                'name': 'AccountId',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'Household__c': {
                'name': 'Household__c',
                'type': 'reference',
                'referenceTo': ['Account']
            },
            'Event__c': {
                'name': 'Event__c',
                'type': 'reference',
                'referenceTo': ['Event__c']
            }
        })
        oc.get_sobject_list = Mock(return_value=['Account', 'Contact'])

        step = amaxa.ExtractionStep('Contact', amaxa.ExtractionScope.DESCENDENTS, ['Name', 'AccountId', 'Household__c'])
        step.perform_lookup_pass = Mock()
        oc.add_step(step)

        step.execute()

        step.perform_lookup_pass.assert_has_calls(
            [
                unittest.mock.call('AccountId'),
                unittest.mock.call('Household__c')
            ],
            any_order=True
        )

    def test_execute_resolves_self_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            },
            'ParentId': {
                'name': 'ParentId',
                'type': 'reference',
                'referenceTo': [
                    'Account'
                ]
            }
        })
        oc.get_extracted_ids = Mock(
            side_effect=[
                set([amaxa.SalesforceId('001000000000001')]),
                set([amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000002')]),
                set([amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000002')]),
                set([amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000002')])
            ]
        )

        step = amaxa.ExtractionStep(
            'Account',
            amaxa.ExtractionScope.QUERY,
            ['Name', 'ParentId'],
            'Name = \'ACME\''
        )
        step.perform_bulk_api_pass = Mock()
        step.perform_lookup_pass = Mock()
        step.resolve_registered_dependencies = Mock()
        oc.add_step(step)
        step.scan_fields()

        self.assertEqual(set(['ParentId']), step.self_lookups)

        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with('SELECT Name, ParentId FROM Account WHERE Name = \'ACME\'')
        oc.get_extracted_ids.assert_has_calls(
            [
                unittest.mock.call('Account'),
                unittest.mock.call('Account'),
                unittest.mock.call('Account'),
                unittest.mock.call('Account')
            ]
        )
        step.perform_lookup_pass.assert_has_calls(
            [
                unittest.mock.call('ParentId'),
                unittest.mock.call('ParentId')
            ]
        )
        step.resolve_registered_dependencies.assert_has_calls(
            [
                unittest.mock.call(),
                unittest.mock.call()
            ]
        )

    def test_execute_does_not_trace_self_lookups_without_trace_all(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            },
            'ParentId': {
                'name': 'ParentId',
                'type': 'reference',
                'referenceTo': [
                    'Account'
                ]
            }
        })
        oc.get_extracted_ids = Mock()

        step = amaxa.ExtractionStep(
            'Account',
            amaxa.ExtractionScope.QUERY,
            ['Name', 'ParentId'],
            'Name = \'ACME\'',
            amaxa.SelfLookupBehavior.TRACE_NONE
        )

        step.perform_bulk_api_pass = Mock()
        step.perform_lookup_pass = Mock()
        step.resolve_registered_dependencies = Mock()

        oc.add_step(step)

        step.execute()

        self.assertEqual(set(['ParentId']), step.self_lookups)
        step.resolve_registered_dependencies.assert_called_once_with()
        oc.get_extracted_ids.assert_not_called()

class test_LoadOperation(unittest.TestCase):
    def test_stores_output_files(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        op.set_input_file('Account', 'a')
        self.assertEqual('a', op.get_input_file('Account'))
    
    def test_maps_record_ids(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        op.register_new_id(amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000001'))

        self.assertEqual(amaxa.SalesforceId('001000000000001'), op.get_new_id(amaxa.SalesforceId('001000000000000')))
    
    def test_execute_runs_all_passes(self):
        connection = Mock()
        first_step = Mock()
        second_step = Mock()

        op = amaxa.LoadOperation(connection)

        op.add_step(first_step)
        op.add_step(second_step)

        op.execute()

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_called_once_with()

        second_step.execute.assert_called_once_with()
        second_step.execute_dependent_updates.assert_called_once_with()


class test_LoadStep(unittest.TestCase):
    def test_stores_lookup_behaviors(self):
        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])

        self.assertEqual(amaxa.OutsideLookupBehavior.INCLUDE, l.get_lookup_behavior_for_field('ParentId'))

        l.set_lookup_behavior_for_field('ParentId', amaxa.OutsideLookupBehavior.ERROR)
        self.assertEqual(amaxa.OutsideLookupBehavior.ERROR, l.get_lookup_behavior_for_field('ParentId'))

    def test_get_value_for_lookup_with_parent_available(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.register_new_id(amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000001'))

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        self.assertEqual(
            l.get_value_for_lookup('ParentId', '001000000000000', '001000000000002'),
            str(amaxa.SalesforceId('001000000000001'))
        )

    def test_get_value_for_lookup_with_include_behavior(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        self.assertEqual(
            l.get_value_for_lookup('ParentId', '001000000000000', '001000000000002'),
            '001000000000000'
        )

    def test_get_value_for_lookup_with_drop_behavior(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        l.set_lookup_behavior_for_field('ParentId', amaxa.OutsideLookupBehavior.DROP_FIELD)

        self.assertEqual(
            l.get_value_for_lookup('ParentId', '001000000000000', '001000000000002'),
            ''
        )

    def test_get_value_for_lookup_with_error_behavior(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        l.set_lookup_behavior_for_field('ParentId', amaxa.OutsideLookupBehavior.ERROR)


        with self.assertRaises(Exception, msg='{} {} has an outside reference in field {} ({}), which is not allowed by the extraction configuration.'.format(
                    'Account',
                    '001000000000002',
                    'ParentId',
                    '001000000000000'
                )
            ):
            l.get_value_for_lookup('ParentId', '001000000000000', '001000000000002')

    def test_populates_lookups(self):
        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.get_value_for_lookup = Mock(return_value='001000000000002')

        record = {
            'Id': '001000000000000',
            'Name': 'Test',
            'ParentId': '001000000000001'
        }

        self.assertEqual(
            {
                'Id': '001000000000000',
                'Name': 'Test',
                'ParentId': '001000000000002'
            },
            l.populate_lookups(record, ['ParentId'], '001000000000000')
        )

    def test_converts_data_for_bulk_api(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(return_value={
            'Name': { 'soapType': 'xsd:string' },
            'Boolean__c': { 'soapType': 'xsd:boolean' },
            'Id': { 'soapType': 'tns:ID' },
            'Date__c': { 'soapType': 'xsd:date' },
            'DateTime__c': { 'soapType': 'xsd:dateTime' },
            'Int__c': { 'soapType': 'xsd:int' },
            'Double__c': { 'soapType': 'xsd:double' },
            'Random__c': { 'soapType': 'xsd:string' }
        })

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        record = {
            'Name': 'Test',
            'Boolean__c': 'yes',
            'Id': '001000000000001',
            'Date__c': '2018-12-31',
            'DateTime__c': '2018-12-31T00:00:00.000Z',
            'Int__c': '100',
            'Double__c': '10.1',
            'Random__c': ''
        }

        self.assertEqual(
            {
                'Name': 'Test',
                'Boolean__c': 'true',
                'Id': '001000000000001',
                'Date__c': '2018-12-31',
                'DateTime__c': '2018-12-31T00:00:00.000Z',
                'Int__c': '100',
                'Double__c': '10.1',
                'Random__c': None
            },
            l.primitivize(record)
        )

    def test_transform_records_calls_context_mapper(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.mappers['Account'] = Mock()
        op.mappers['Account'].transform_record = Mock(
            return_value={
                'Name': 'Test2',
                'ParentId': '001000000000001'
            }
        )

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        self.assertEqual(
            {
                'Name': 'Test2',
                'ParentId': '001000000000001'
            },
            l.transform_record(
                {
                    'Name': 'Test1',
                    'ParentId': '001000000000000'
                }
            )
        )
        op.mappers['Account'].transform_record.assert_called_once_with(
            {
                'Name': 'Test1',
                'ParentId': '001000000000000'
            }
        )

    def test_transform_records_cleans_excess_fields(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        l = amaxa.LoadStep('Account', ['Name', 'ParentId'])
        l.context = op

        self.assertEqual(
            {
                'Name': 'Test2',
                'ParentId': '001000000000001'
            },
            l.transform_record(
                {
                    'Name': 'Test2',
                    'ParentId': '001000000000001',
                    'Excess__c': True
                }
            )
        )

    def test_execute_transforms_and_loads_records_without_lookups(self):
        record_list = [
            { 'Name': 'Test', 'Id': '001000000000000' },
            { 'Name': 'Test 2', 'Id': '001000000000001' }
        ]
        clean_record_list = [
            { 'Name': 'Test' },
            { 'Name': 'Test 2' }
        ]
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(return_value={
            'Name': { 'type': 'string '},
            'Id': { 'type': 'string' }
        })
        op.register_new_id = Mock()
        op.get_input_file = Mock(
            return_value=record_list
        )
        account_proxy = Mock()
        op.get_bulk_proxy_object = Mock(return_value=account_proxy)
        account_proxy.insert = Mock(
            return_value=[
                { 'success': True, 'id': '001000000000002' },
                { 'success': True, 'id': '001000000000003' }
            ]
        )
        op.mappers['Account'] = Mock()
        op.mappers['Account'].transform_record = Mock(side_effect=lambda x: x)

        l = amaxa.LoadStep('Account', ['Name'])
        l.context = op
        l.primitivize = Mock(side_effect=lambda x: x)
        l.populate_lookups = Mock(side_effect=lambda x, y, z: x)

        l.scan_fields()
        l.execute()

        op.mappers['Account'].transform_record.assert_has_calls([unittest.mock.call(x) for x in clean_record_list])
        l.primitivize.assert_has_calls([unittest.mock.call(x) for x in clean_record_list])
        l.populate_lookups.assert_has_calls(
            [unittest.mock.call(x, set(), y['Id']) for (x, y) in zip(clean_record_list, record_list)]
        )

        op.get_bulk_proxy_object.assert_called_once_with('Account')
        account_proxy.insert.assert_called_once_with(clean_record_list)
        op.register_new_id.assert_has_calls(
            [
                unittest.mock.call(amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000002')),
                unittest.mock.call(amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000003'))
            ]
        )

    def test_execute_transforms_and_loads_records_with_lookups(self):
        record_list = [
            { 'Name': 'Test', 'Id': '001000000000000', 'Lookup__c': '003000000000000' },
            { 'Name': 'Test 2', 'Id': '001000000000001', 'Lookup__c': '003000000000001'}
        ]
        clean_record_list = [
            { 'Name': 'Test', 'Lookup__c': '003000000000000' },
            { 'Name': 'Test 2', 'Lookup__c': '003000000000001'}
        ]
        transformed_record_list = [
            { 'Name': 'Test', 'Lookup__c': str(amaxa.SalesforceId('003000000000002')) },
            { 'Name': 'Test 2', 'Lookup__c': str(amaxa.SalesforceId('003000000000003')) }
        ]


        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(return_value={
            'Name': { 'type': 'string '},
            'Id': { 'type': 'string' },
            'Lookup__c': { 'type': 'string' }
        })

        op.register_new_id(amaxa.SalesforceId('003000000000000'), amaxa.SalesforceId('003000000000002'))
        op.register_new_id(amaxa.SalesforceId('003000000000001'), amaxa.SalesforceId('003000000000003'))

        op.register_new_id = Mock()
        op.get_input_file = Mock(
            return_value=record_list
        )
        account_proxy = Mock()
        op.get_bulk_proxy_object = Mock(return_value=account_proxy)
        account_proxy.insert = Mock(
            return_value=[
                { 'success': True, 'id': '001000000000002' },
                { 'success': True, 'id': '001000000000003' }
            ]
        )
        op.mappers['Account'] = Mock()
        op.mappers['Account'].transform_record = Mock(side_effect=lambda x: x)

        l = amaxa.LoadStep('Account', ['Name', 'Lookup__c'])
        l.context = op
        l.primitivize = Mock(side_effect=lambda x: x)

        l.scan_fields()
        l.descendent_lookups = set(['Lookup__c'])

        l.execute()

        op.mappers['Account'].transform_record.assert_has_calls([unittest.mock.call(x) for x in clean_record_list])
        l.primitivize.assert_has_calls([unittest.mock.call(x) for x in transformed_record_list])

        op.get_bulk_proxy_object.assert_called_once_with('Account')
        account_proxy.insert.assert_called_once_with(transformed_record_list)
        op.register_new_id.assert_has_calls(
            [
                unittest.mock.call(amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000002')),
                unittest.mock.call(amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000003'))
            ]
        )

    def test_execute_handles_errors(self):
        record_list = [
            { 'Name': 'Test', 'Id': '001000000000000' },
            { 'Name': 'Test 2', 'Id': '001000000000001' }
        ]
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(return_value={
            'Name': { 'type': 'string '},
            'Id': { 'type': 'string' }
        })
        op.register_new_id = Mock()
        op.get_input_file = Mock(
            return_value=record_list
        )
        account_proxy = Mock()
        op.get_bulk_proxy_object = Mock(return_value=account_proxy)
        account_proxy.insert = Mock(
            return_value=[
                { 'success': False, 'id': '001000000000002' },
                { 'success': False, 'id': '001000000000003' }
            ]
        )

        l = amaxa.LoadStep('Account', ['Name'])
        l.context = op

        l.scan_fields()
        with self.assertRaises(Exception, msg='Failed to load {} {}'.format(
            'Account',
            record_list[0]['Id']
        )):
            l.execute()

    def test_execute_dependent_updates_handles_lookups(self):
        record_list = [
            { 'Name': 'Test', 'Id': '001000000000000', 'Lookup__c': '001000000000001' },
            { 'Name': 'Test 2', 'Id': '001000000000001', 'Lookup__c': '001000000000000'}
        ]
        transformed_record_list = [
            { 'Id': '001000000000000', 'Lookup__c': str(amaxa.SalesforceId('001000000000003')) },
            { 'Id': '001000000000001', 'Lookup__c': str(amaxa.SalesforceId('001000000000002')) }
        ]

        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(return_value={
            'Name': { 'type': 'string '},
            'Id': { 'type': 'string' },
            'Lookup__c': { 'type': 'string' }
        })

        op.register_new_id(amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000002'))
        op.register_new_id(amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000003'))

        op.register_new_id = Mock()
        op.get_input_file = Mock(
            return_value=record_list
        )
        account_proxy = Mock()
        op.get_bulk_proxy_object = Mock(return_value=account_proxy)
        account_proxy.update = Mock(
            return_value=[
                { 'success': True },
                { 'success': True }
            ]
        )

        l = amaxa.LoadStep('Account', ['Name', 'Lookup__c'])
        l.context = op

        l.scan_fields()
        l.self_lookups = set(['Lookup__c'])

        l.execute_dependent_updates()

        op.get_bulk_proxy_object.assert_called_once_with('Account')
        account_proxy.update.assert_called_once_with(transformed_record_list)

    def test_execute_dependent_updates_handles_errors(self):
        record_list = [
            { 'Name': 'Test', 'Id': '001000000000000', 'Lookup__c': '001000000000001' },
            { 'Name': 'Test 2', 'Id': '001000000000001', 'Lookup__c': '001000000000000'}
        ]

        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(return_value={
            'Name': { 'type': 'string '},
            'Id': { 'type': 'string' },
            'Lookup__c': { 'type': 'string' }
        })

        op.register_new_id(amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000002'))
        op.register_new_id(amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000003'))

        op.register_new_id = Mock()
        op.get_input_file = Mock(
            return_value=record_list
        )
        account_proxy = Mock()
        op.get_bulk_proxy_object = Mock(return_value=account_proxy)
        account_proxy.update = Mock(
            return_value=[
                { 'success': False },
                { 'success': False }
            ]
        )

        l = amaxa.LoadStep('Account', ['Name', 'Lookup__c'])
        l.context = op

        l.scan_fields()
        l.self_lookups = set(['Lookup__c'])
        with self.assertRaises(Exception, msg='Failed to execute dependent updates for {} {}'.format(
            'Account',
            '001000000000000'
        )):
            l.execute_dependent_updates()

if __name__ == "__main__":
    unittest.main()