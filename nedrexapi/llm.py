from langchain_community.embeddings.ollama import OllamaEmbeddings
from langchain_ollama.chat_models import ChatOllama
from nedrexapi.config import config as _config
from langchain_community.llms.ollama import Ollama

vars = None

def init():
    global vars
    vars = dict()
    vars["_LLM_BASE"]=_config["embeddings.server_base"]
    vars["_LLM_model"]=_config[f"embeddings.model"]
    vars["_LLM_path"]=_config[f"embeddings.path"]

    vars["_LLM_chat_model"]=_config["chat.model"]
    vars["_LLM_chat_base"]=_config["chat.server_base"]
    vars["_LLM_chat_api_key"]=_config["chat.api_key"]

    vars["headers"] = {"Authorization": "Bearer " + vars["_LLM_chat_api_key"]}


def get_embedder():
    if vars is None:
        init()
    return OllamaEmbeddings(base_url=vars["_LLM_BASE"], model=vars["_LLM_model"], headers=vars["headers"])

def get_generator():
    if vars is None:
        init()
    return Ollama(base_url=vars["_LLM_chat_base"], model=vars["_LLM_chat_model"], temperature=0.0, headers=vars["headers"])

def get_chat():
    if vars is None:
        init()
    return ChatOllama(base_url=vars["_LLM_chat_base"], model=vars["_LLM_chat_model"], temperature=0.0, client_kwargs={'headers': vars["headers"]})

def get_embedding(query):
    embedder = get_embedder()
    return embedder.embed(query)



def generate(query):
    ollama_llm = get_generator()
    response = ollama_llm.invoke(query)
    return response


def chat(messages):
    llm = get_chat()
    response = llm.invoke(input=messages)
    return response
