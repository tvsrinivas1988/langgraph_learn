import os
from typing import TypedDict, Literal
from IPython.display import Image, display
from langgraph.graph import StateGraph, START, END
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import MessagesState
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from langchain_groq import ChatGroq
from dotenv import load_dotenv

load_dotenv()

def get_latest_error(job_name: str) -> dict:
    """
    Connects to PostgreSQL, queries error_logs for the latest error for a given job, 
    and returns the result.
    Args:
    job_name : Airflow job name that as failed
    """

    # Connection details (could also be read from environment or secrets.toml)
    DB_USER = os.getenv("DB_USER", "your_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "your_db")

    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        "?sslmode=require"
    )

    engine = create_engine(conn_str)

    query = text("""
        SELECT *
        FROM error_logs
        WHERE job_name = :job_name
        ORDER BY created_at 
        LIMIT 1;
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"job_name": job_name}).fetchone()
            if result:
                return dict(result._mapping)  # Convert Row to dict
            else:
                return {"message": f"No errors found for job: {job_name}"}

    except SQLAlchemyError as e:
        return {"error": f"SQLAlchemy error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
    finally:
        engine.dispose()

groq_api_key = os.getenv("GROQ_API_KEY")



    
def llm_node(state: MessagesState):
    print("---LLM Node----")
    llm = ChatGroq(
                model="openai/gpt-oss-20b",
                temperature=0,
                api_key=groq_api_key,
                max_tokens=None,
                reasoning_format="parsed",
                timeout=None,
                max_retries=2
                )
    prompt = ChatPromptTemplate.from_messages(
[
    (
        "system",
        "You are a helpful L3 IT Ops assistant that bifurcates the  {input} to one of these 3 : DI module , DP Module or DQ Module. If the module name starts with DI then its DI and likewise . If you dont know you say Dummy Module. If you are asked to find the latest error for the job use the tool call available." 
        ,
    ),
    ("human", "{input}"),
]
)
    llm_with_tools  = llm.bind_tools([get_latest_error])
    chain = prompt | llm_with_tools
    llm_resp=chain.invoke(
    {
        
        "input": state["messages"]
    }
    )
    return {"messages": llm_resp}

builder = StateGraph(MessagesState)
builder.add_node("llm_node", llm_node)

builder.add_edge(START, "llm_node")
builder.add_edge("llm_node", END)

graph = builder.compile()
##display(Image(graph.get_graph().draw_mermaid_png()))



if __name__ == "__main__":
    final_state=graph.invoke({"messages": "DIJB001"})
    for m in final_state['messages']:
        m.pretty_print()

    final_state=graph.invoke({"messages": "What is the latest error for customer_ingestion"})
    for m in final_state['messages']:
        m.pretty_print()