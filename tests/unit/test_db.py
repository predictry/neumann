__author__ = 'guilherme'


import unittest
import uuid
from neumann.core.db import neo4j
from neumann.core import errors


class Neo4jTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_001_should_connect_to_db(self):

        db_conn = None

        assert db_conn is None
        db_conn = neo4j.get_connection()
        assert db_conn is not None


class Neo4jNodeRelationshipTest(unittest.TestCase):

    john = dict(name="John Doe", age=45, uuid=str(uuid.uuid4()))
    jane = dict(name="Jane Doe", age=45, uuid=str(uuid.uuid4()))
    label = "TestPersona"
    relationship = "MARRIED"
    relationship_props = {"date": "July 14, 1567", "location": "Blooming Hills, Arcania"}

    invalid_labels = [
        "Label With Space",
        "1_label_starting_with_a_number",
        "&_starting_with_special_char",
        "label_with_spec_char_&",
        "$%",
        "5"]

    valid_labels = [
        "label_with_underscores",
        "l1abel_2_with_numbers_3"
    ]

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_001_should_create_node(self):
        assert isinstance(self.jane, dict)
        assert isinstance(self.john, dict)
        assert isinstance(self.label, str)
        assert neo4j.is_node_exists(self.label, "name", self.jane["name"]) is False
        assert neo4j.is_node_exists(self.label, "name", self.john["name"]) is False

        n_jane = neo4j.create_node(self.jane, [self.label])
        n_john = neo4j.create_node(self.john, [self.label])

        assert n_jane is not None
        assert n_john is not None

        for key in n_jane.properties:
            assert n_jane.properties[key] == self.jane[key]
        for key in n_john.properties:
            assert n_john.properties[key] == self.john[key]

        assert neo4j.is_node_exists(self.label, "name", self.jane["name"]) is True
        assert neo4j.is_node_exists(self.label, "name", self.john["name"]) is True

    def test_002_should_verify_node_properties(self):

        assert neo4j.is_node_exists(self.label, "name", self.jane["name"]) is True
        assert neo4j.is_node_exists(self.label, "name", self.john["name"]) is True

        n_jane = neo4j.get_node(self.label, "name", self.jane["name"])
        n_john = neo4j.get_node(self.label, "name", self.john["name"])

        assert n_jane is not None
        assert n_john is not None

        props_jane = n_jane.properties
        props_john = n_john.properties
        labels_jane = n_jane.labels
        labels_john = n_john.labels

        for k in props_jane:
            assert props_jane[k] == self.jane[k]
        for k in props_john:
            assert props_john[k] == self.john[k]

        assert self.label in labels_jane
        assert self.label in labels_john

    def test_003_should_create_a_relationship(self):
        n_jane = neo4j.get_node(self.label, "name", self.jane["name"])
        n_john = neo4j.get_node(self.label, "name", self.john["name"])

        r = neo4j.create_relationship(n_jane, self.relationship, n_john, self.relationship_props)

        assert r is not None
        assert r.start_node == n_jane
        assert r.end_node == n_john

        for k in r.start_node.properties:
            assert r.start_node.properties[k] == n_jane.properties[k]
        for k in r.end_node.properties:
            assert r.end_node.properties[k] == n_john.properties[k]

    def test_004_should_verify_relationship_properties(self):
        n_jane = neo4j.get_node(self.label, "name", self.jane["name"])
        n_john = neo4j.get_node(self.label, "name", self.john["name"])

        assert n_jane is not None
        assert n_john is not None

        r = neo4j.create_relationship(n_jane, self.relationship, n_john, self.relationship_props)

        for k in r.properties:
            assert r.properties[k] == self.relationship_props[k]

    def test_005_should_delete_relationship(self):
        n_jane = neo4j.get_node(self.label, "name", self.jane["name"])
        n_john = neo4j.get_node(self.label, "name", self.john["name"])

        assert n_jane is not None
        assert n_john is not None

        r = neo4j.create_relationship(n_jane, self.relationship, n_john, self.relationship_props)

        assert neo4j.delete_relationship(r) is True
        assert r.exists is False

    def test_006_should_validate_labels(self):

        for label in self.invalid_labels:
            assert neo4j.is_valid_label(label) is False

        for label in self.valid_labels:
            assert neo4j.is_valid_label(label) is True

    def test_007_should_delete_nodes(self):
        n_jane = neo4j.get_node(self.label, "name", self.jane["name"])
        n_john = neo4j.get_node(self.label, "name", self.john["name"])

        assert neo4j.delete_node(n_jane) is True
        assert neo4j.delete_node(n_john) is True
        assert n_jane.exists is False
        assert n_john.exists is False

    def test_008_should_fail_to_create_node(self):

        for label in self.invalid_labels:
            self.assertRaises(errors.InvalidLabelError, neo4j.create_node, self.jane, label)

    def test_009_should_fail_to_check_node(self):

        for label in self.invalid_labels:
            self.assertRaises(errors.InvalidLabelError, neo4j.is_node_exists, label, "name", "random")


class Neo4jTraversePathTest(unittest.TestCase):

        label = "Alphabet"
        trail = {
            "A": ["B", "C"],
            "B": ["D"],
            "C": ["E"],
            "D": ["F"],
            "E": ["G"],
            "F": ["H"],
            "G": ["H"],
            "H": ["I", "J"],
            "I": [],
            "J": []
        }

        #todo: fix this test 3to2

        def test_001_should_create_nodes_and_connect_them(self):

            nodes = [{"id": id} for id in list(map(chr, range(65, 75)))]

            n_nodes = {}

            for node in nodes:
                n_letter = neo4j.create_node(node, [self.label])
                n_nodes[node["id"]] = n_letter

                assert n_letter is not None
                assert n_letter.properties["id"] == node["id"]

            for k_n_letter in n_nodes:

                for k in self.trail[n_nodes[k_n_letter].properties["id"]]:

                    assert neo4j.create_relationship(n_nodes[k_n_letter], "BRIDGED_TO", n_nodes[k]) is not None

        def test_002_should_traverse_path_and_find_all_nodes(self):

            nodes = [{"id": id} for id in list(map(chr, range(65, 75)))]

            n_start = neo4j.get_node(self.label, key="id", value=nodes[0]["id"])

            n_nodes = neo4j.traverse_path(n_start, "BRIDGED_TO")

            assert len(n_nodes) == 9

            path_letters = list(map(chr, range(66, 75)))

            for n_node in n_nodes:

                assert n_node.properties["id"] in path_letters

        def test_003_should_delete_nodes_in_path(self):

            nodes = [{"id": id} for id in list(map(chr, range(65, 75)))]

            n_start = neo4j.get_node(self.label, key="id", value=nodes[0]["id"])

            n_nodes = neo4j.traverse_path(n_start, "BRIDGED_TO")

            for n_node in n_nodes:
                assert neo4j.delete_node(n_node) is True

        def test_004_should_verify_nodes_were_deleted(self):

            path_letters = list(map(chr, range(66, 75)))

            for letter in path_letters:

                assert neo4j.is_node_exists(self.label, key="name", value=letter) is False

        def test_005_should_delete_leading_node(self):

            n = neo4j.get_node(self.label, key="id", value="A")

            assert n is not None
            assert neo4j.delete_node(n) is True
            assert neo4j.is_node_exists(self.label, key="id", value="A") is False


if __name__ == "__main__":
    unittest.main()