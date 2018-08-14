import unittest
import amaxa
from unittest.mock import Mock

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

            self.assertEqual(known_good_ids[id_15], amaxa.SalesforceId(known_good_ids[id_15]))
            self.assertEqual(known_good_ids[id_15], str(amaxa.SalesforceId(known_good_ids[id_15])))

            self.assertEqual(hash(known_good_ids[id_15]), hash(amaxa.SalesforceId(id_15)))
    
    def test_raises_valueerror(self):
        with self.assertRaises(ValueError):
            bad_id = amaxa.SalesforceId('test')

class test_OperationContext(unittest.TestCase):
    def test_tracks_dependencies(self):
        connection = Mock()

        oc = amaxa.OperationContext(
            connection,
            ['Account']
        )

        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_dependencies('Account'))

    def test_creates_and_caches_proxy_objects(self):
        connection = Mock()
        connection.SFType = Mock(return_value='Account')

        oc = amaxa.OperationContext(
            connection,
            ['Account']
        )

        proxy = oc.get_proxy_object('Account')

        self.assertEqual('Account', proxy)
        connection.SFType.assert_called_once_with('Account')

        connection.reset_mock()
        proxy = oc.get_proxy_object('Account')

        # Proxy should be cached
        self.assertEqual('Account', proxy)
        connection.SFType.assert_not_called()
    
    def test_caches_describe_results(self):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }
        account_mock.describe = Mock(return_value=describe_info)
        connection.SFType = Mock(return_value=account_mock)

        oc = amaxa.OperationContext(
            connection,
            ['Account']
        )

        retval = oc.get_describe('Account')
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_describe('Account')
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_not_called()
    
    def test_caches_field_maps(self):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }
        account_mock.describe = Mock(return_value=describe_info)
        connection.SFType = Mock(return_value=account_mock)

        oc = amaxa.OperationContext(
            connection,
            ['Account']
        )

        retval = oc.get_field_map('Account')
        self.assertEqual({ 'Name': { 'name': 'Name' }, 'Id': { 'name': 'Id' } }, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_field_map('Account')
        self.assertEqual({ 'Name': { 'name': 'Name' }, 'Id': { 'name': 'Id' } }, retval)
        account_mock.describe.assert_not_called()
    
    def test_filters_field_maps(self):
        connection = Mock()
        account_mock = Mock()

        fields = [{ 'name': 'Name' }, { 'name': 'Id' }]
        describe_info = { 'fields' : fields }
        account_mock.describe = Mock(return_value=describe_info)
        connection.SFType = Mock(return_value=account_mock)

        oc = amaxa.OperationContext(
            connection,
            ['Account']
        )

        retval = oc.get_filtered_field_map('Account', lambda f: f['name'] == 'Id')
        self.assertEqual({ 'Id': { 'name': 'Id' } }, retval)


if __name__ == "__main__":
    unittest.main()