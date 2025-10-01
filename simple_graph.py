from typing import TypedDict, Literal
from IPython.display import Image, display
from langgraph.graph import StateGraph, START, END

def simple_graph(afl_job_name: str):
    class State(TypedDict):
        afl_job_name: str
        router_node_accessed: str
        di_node_accessed: str
        dp_nodes_accessed: str
        dq_node_accessed: str
        dummy_node_accessed: str

    def router_node(state):
        print("---Router Node----")
        return {"router_node_accessed": "Y"}

    def di_node(state):
        print("---Data Ingestion Node----")
        return {"di_node_accessed": "Y"}

    def dp_node(state):
        print("---Data Processing Node----")
        return {"dp_nodes_accessed": "Y"}

    def dq_node(state):
        print("---Data Quality Node----")
        return {"dq_node_accessed": "Y"}

    def dummy_node(state):
        print("---Dummy Node----")
        return {"dummy_node_accessed": "Y"}

    def router_out(state) -> Literal["di_node", "dp_node", "dq_node", "dummy_node"]:
        if "DI" in state["afl_job_name"]:
            return "di_node"
        elif "DP" in state["afl_job_name"]:
            return "dp_node"
        elif "DQ" in state["afl_job_name"]:
            return "dq_node"
        else:
            return "dummy_node"

    builder = StateGraph(State)
    builder.add_node("router_node", router_node)
    builder.add_node("di_node", di_node)
    builder.add_node("dp_node", dp_node)
    builder.add_node("dq_node", dq_node)
    builder.add_node("dummy_node", dummy_node)

    builder.add_edge(START, "router_node")
    builder.add_conditional_edges(
        "router_node",
        router_out,
        ["di_node", "dp_node", "dq_node", "dummy_node"]
    )
    builder.add_edge("di_node", "dummy_node")
    builder.add_edge("dp_node", "dummy_node")
    builder.add_edge("dq_node", "dummy_node")
    builder.add_edge("dummy_node", END)

    graph = builder.compile()
    ##display(Image(graph.get_graph().draw_mermaid_png()))
    final_state=graph.invoke({"afl_job_name": afl_job_name})
    return final_state

if __name__ == "__main__":
    simple_graph("DIJB0001")
