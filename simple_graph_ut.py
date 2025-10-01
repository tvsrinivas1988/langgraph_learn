import unittest
from langgraph.graph import StateGraph, START, END
from simple_graph import simple_graph
class TestSimpleGraph(unittest.TestCase):
    def test_di_route(self):
        # "DI" in job name → should pass DI node
        result = simple_graph("DI123")
        self.assertEqual(result["di_node_accessed"], "Y")
        self.assertEqual(result["dummy_node_accessed"], "Y")

    def test_dp_route(self):
        # "DP" in job name → should pass DP node
        result = simple_graph("DP456")
        self.assertEqual(result["dp_nodes_accessed"], "Y")
        self.assertEqual(result["dummy_node_accessed"], "Y")

    def test_dq_route(self):
        # "DQ" in job name → should pass DQ node
        result = simple_graph("DQ789")
        self.assertEqual(result["dq_node_accessed"], "Y")
        self.assertEqual(result["dummy_node_accessed"], "Y")

    def test_dummy_route(self):
        # no DI/DP/DQ → should go directly to Dummy
        result = simple_graph("OTHER001")
        self.assertEqual(result["dummy_node_accessed"], "Y")

if __name__ == "__main__":
    unittest.main()
