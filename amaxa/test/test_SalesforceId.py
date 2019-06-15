import unittest
from .. import amaxa


class test_SalesforceId(unittest.TestCase):
    def test_converts_real_id_pairs(self):
        known_good_ids = {
            "01Q36000000RXX5": "01Q36000000RXX5EAO",
            "005360000016xkG": "005360000016xkGAAQ",
            "01I36000002zD9R": "01I36000002zD9REAU",
            "0013600001ohPTp": "0013600001ohPTpAAM",
            "0033600001gyv5B": "0033600001gyv5BAAQ",
        }

        for id_15 in known_good_ids:
            self.assertEqual(known_good_ids[id_15], str(amaxa.SalesforceId(id_15)))
            self.assertEqual(known_good_ids[id_15], amaxa.SalesforceId(id_15))
            self.assertEqual(amaxa.SalesforceId(id_15), known_good_ids[id_15])

            self.assertEqual(id_15, amaxa.SalesforceId(id_15))
            self.assertNotEqual(id_15, str(amaxa.SalesforceId(id_15)))

            self.assertEqual(amaxa.SalesforceId(id_15), amaxa.SalesforceId(id_15))
            self.assertEqual(
                amaxa.SalesforceId(str(amaxa.SalesforceId(id_15))),
                amaxa.SalesforceId(str(amaxa.SalesforceId(id_15))),
            )

            self.assertEqual(
                known_good_ids[id_15], amaxa.SalesforceId(known_good_ids[id_15])
            )
            self.assertEqual(
                known_good_ids[id_15], str(amaxa.SalesforceId(known_good_ids[id_15]))
            )

            self.assertEqual(
                hash(known_good_ids[id_15]), hash(amaxa.SalesforceId(id_15))
            )

    def test_raises_valueerror(self):
        with self.assertRaises(ValueError):
            # pylint: disable=W0612
            bad_id = amaxa.SalesforceId("test")

    def test_equals_other_id(self):
        the_id = amaxa.SalesforceId("001000000000000")

        self.assertEqual(the_id, amaxa.SalesforceId(the_id))

    def test_does_not_equal_other_value(self):
        the_id = amaxa.SalesforceId("001000000000000")

        self.assertNotEqual(the_id, 1)

    def test_str_repr_equal_18_char_id(self):
        the_id = amaxa.SalesforceId("001000000000000")

        self.assertEqual(the_id.id, str(the_id))
        self.assertEqual(the_id.id, repr(the_id))

    def test_hashing(self):
        id_set = set()
        for i in range(400):
            new_id = amaxa.SalesforceId("001000000000" + str(i + 1).zfill(3))
            self.assertNotIn(new_id, id_set)
            id_set.add(new_id)
            self.assertIn(new_id, id_set)
