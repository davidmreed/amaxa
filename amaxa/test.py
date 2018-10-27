import unittest
import amaxa
import simple_salesforce
from unittest.mock import Mock, PropertyMock, patch

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

    def test_hashing(self):
        id_set = set()
        for i in range(400):
            new_id = amaxa.SalesforceId('001000000000' + str(i + 1).zfill(3))
            self.assertNotIn(new_id, id_set)
            id_set.add(new_id)
            self.assertIn(new_id, id_set)

class test_OperationContext(unittest.TestCase):
    def test_tracks_dependencies(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_dependencies('Account'))

    def test_doesnt_add_dependency_for_extracted_record(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set(), oc.get_dependencies('Account'))

    def test_creates_and_caches_proxy_objects(self):
        connection = Mock()
        p = PropertyMock(return_value='Account')
        type(connection).Account = p

        oc = amaxa.OperationContext(connection)

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

        oc = amaxa.OperationContext(connection)

        proxy = oc.get_bulk_proxy_object('Account')

        self.assertEqual('Account', proxy)
        p.assert_called_once_with()

        p.reset_mock()
        proxy = oc.get_bulk_proxy_object('Account')

        # Proxy should be cached
        self.assertEqual('Account', proxy)
        p.assert_not_called()

    @patch('amaxa.OperationContext.get_proxy_object')
    def test_caches_describe_results(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.OperationContext(connection)

        retval = oc.get_describe('Account')
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_describe('Account')
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_not_called()

    @patch('amaxa.OperationContext.get_proxy_object')
    def test_caches_field_maps(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.OperationContext(connection)

        retval = oc.get_field_map('Account')
        self.assertEqual({ 'Name': { 'name': 'Name' }, 'Id': { 'name': 'Id' } }, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_field_map('Account')
        self.assertEqual({ 'Name': { 'name': 'Name' }, 'Id': { 'name': 'Id' } }, retval)
        account_mock.describe.assert_not_called()

    @patch('amaxa.OperationContext.get_proxy_object')
    def test_filters_field_maps(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.OperationContext(connection)

        retval = oc.get_filtered_field_map('Account', lambda f: f['name'] == 'Id')
        self.assertEqual({ 'Id': { 'name': 'Id' } }, retval)

    def test_store_result_retains_ids(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.extracted_ids['Account'])

    def test_store_result_writes_records(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })

    def test_store_result_transforms_output(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

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

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set(), oc.get_dependencies('Account'))

    def test_get_extracted_ids_returns_results(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_extracted_ids('Account'))

    def test_get_sobject_ids_for_reference_returns_correct_ids(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
        oc.get_field_map = Mock(return_value={ 'Lookup__c': { 'referenceTo': ['Account', 'Contact'] }})

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'University of Caprica' })
        oc.store_result('Contact', { 'Id': '003000000000000', 'Name': 'Gaius Baltar' })
        oc.store_result('Opportunity', { 'Id': '006000000000000', 'Name': 'Defense Mainframe' })

        self.assertEqual(set([amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('003000000000000')]),
                         oc.get_sobject_ids_for_reference('Account', 'Lookup__c'))


class test_ExtractMapper(unittest.TestCase):
    def test_transform_key_applies_mapping(self):
        pass #FIXME

    def test_transform_value_applies_transformations(self):
        pass #FIXME

    def test_transform_record_does(self):
        pass #FIXME

class test_SingleObjectExtraction(unittest.TestCase):
    def test_identifies_self_lookups(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
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

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c', 'Other__c'], oc)

        self.assertEqual(set(['Lookup__c']), step.self_lookups)

    def test_throws_exception_for_polymorphic_self_lookup(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account', 'Contact']
            },
            'Other__c': {
                'name': 'Other__c',
                'type': 'reference',
                'referenceTo': ['Contact']
            }
        })

        with self.assertRaises(AssertionError):
            # pylint: disable=W0612
            step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c', 'Other__c'], oc)

    def test_generates_field_list(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
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

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c', 'Other__c'], oc)

        self.assertEqual('Lookup__c, Other__c', step.get_field_list())

    def test_store_result_calls_context(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, [], oc)

        step.store_result({ 'Id': '001000000000000', 'Name': 'Picon Fleet Headquarters' })
        oc.store_result.assert_called_once_with('Account', { 'Id': '001000000000000', 'Name': 'Picon Fleet Headquarters' })
        oc.add_dependency.assert_not_called()

    def test_store_result_registers_self_lookup_dependencies(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)

        step.store_result({ 'Id': '001000000000000', 'Lookup__c': '001000000000001', 'Name': 'Picon Fleet Headquarters' })
        oc.add_dependency.assert_called_once_with('Account', '001000000000001')

    def test_perform_lookup_pass_executes_correct_query(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)

        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })
        oc.get_sobject_ids_for_reference = Mock(return_value=set([amaxa.SalesforceId('001000000000000')]))

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)

        step.perform_id_field_pass = Mock()
        step.perform_lookup_pass('Lookup__c')

        oc.get_sobject_ids_for_reference.assert_called_once_with('Account', 'Lookup__c')
        step.perform_id_field_pass.assert_called_once_with('Lookup__c', set([amaxa.SalesforceId('001000000000000')]))

    def test_perform_id_field_pass_queries_all_records(self):
        connection = Mock()
        connection.query_all = Mock(side_effect=lambda x: { 'records': [{ 'Id': '001000000000001'}] })

        oc = amaxa.OperationContext(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)
        step.store_result = Mock()

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

        oc = amaxa.OperationContext(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)
        step.store_result = Mock()

        step.perform_id_field_pass('Lookup__c', set([amaxa.SalesforceId('001000000000001'), amaxa.SalesforceId('001000000000002')]))
        step.store_result.assert_any_call(connection.query_all('Account')['records'][0])
        step.store_result.assert_any_call(connection.query_all('Account')['records'][1])

    def test_perform_id_field_pass_ignores_empty_set(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
        oc.get_field_map = Mock(return_value={
            'Lookup__c': {
                'name': 'Lookup__c',
                'type': 'reference',
                'referenceTo': ['Account']
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)

        step.perform_id_field_pass('Lookup__c', set())

        connection.query_all.assert_not_called()

    def test_perform_bulk_api_pass_performs_query(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
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


        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.QUERY, ['Lookup__c'], oc)
        step.store_result = Mock()

        step.perform_bulk_api_pass('SELECT Id FROM Account')
        oc.get_bulk_proxy_object.assert_called_once_with('Account')
        bulk_proxy.query.assert_called_once_with('SELECT Id FROM Account')

    def test_perform_bulk_api_pass_stores_results(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
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

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)
        step.store_result = Mock()

        step.perform_bulk_api_pass('SELECT Id FROM Account')
        step.store_result.assert_any_call(bulk_proxy.query.return_value[0])
        step.store_result.assert_any_call(bulk_proxy.query.return_value[1])

    def test_resolve_registered_dependencies_loads_records(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
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

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)
        step.perform_id_field_pass = Mock()

        step.resolve_registered_dependencies()

        oc.get_dependencies.assert_has_calls([unittest.mock.call('Account'), unittest.mock.call('Account')])
        step.perform_id_field_pass.assert_called_once_with('Id', set([amaxa.SalesforceId('001000000000001'),
            amaxa.SalesforceId('001000000000002')]))

    def test_resolve_registered_dependencies_throws_exception_for_missing_ids(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
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

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Lookup__c'], oc)
        step.perform_id_field_pass = Mock()

        with self.assertRaises(Exception):
            step.resolve_registered_dependencies()

    def test_execute_with_all_records_performs_bulk_api_pass(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.ALL_RECORDS, ['Name'], oc)
        step.perform_bulk_api_pass = Mock()

        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with('SELECT Name FROM Account')

    def test_execute_with_query_performs_bulk_api_pass(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            }
        })

        step = amaxa.SingleObjectExtraction('Account', amaxa.ExtractionScope.QUERY, ['Name'], oc, 'Name != null')
        step.perform_bulk_api_pass = Mock()

        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with('SELECT Name FROM Account WHERE Name != null')

    def test_execute_loads_all_descendents(self):
        connection = Mock()

        oc = amaxa.OperationContext(connection)
        oc.get_field_map = Mock(return_value={
            'Name': {
                'name': 'Name',
                'type': 'text'
            },
            'AccountId': {
                'name': 'AccountId',
                'type': 'reference',
                'referenceTo': 'Account'
            },
            'Household__c': {
                'name': 'Household__c',
                'type': 'reference',
                'referenceTo': 'Account'
            },
            'Event__c': {
                'name': 'Event__c',
                'type': 'reference',
                'referenceTo': 'Event__c'
            }
        })

        step = amaxa.SingleObjectExtraction('Contact', amaxa.ExtractionScope.DESCENDENTS, ['Name', 'AccountId', 'Household__c'], oc)
        step.perform_lookup_pass = Mock()

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

        oc = amaxa.OperationContext(connection)
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

        step = amaxa.SingleObjectExtraction(
            'Account',
            amaxa.ExtractionScope.QUERY,
            ['Name', 'ParentId'],
            oc,
            'Name = \'ACME\''
        )
        step.perform_bulk_api_pass = Mock()
        step.perform_lookup_pass = Mock()
        step.resolve_registered_dependencies = Mock()

        self.assertEquals(set(['ParentId']), step.self_lookups)

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

class test_MultiObjectExtraction(unittest.TestCase):
    pass #FIXME


if __name__ == "__main__":
    unittest.main()