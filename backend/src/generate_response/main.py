import os, json
import boto3
from aws_lambda_powertools import Logger
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain.memory import ConversationBufferMemory
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.chains import ConversationalRetrievalChain
from langchain_community.chat_models import BedrockChat
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains import create_retrieval_chain
from langchain import hub

MEMORY_TABLE = os.environ["MEMORY_TABLE"]
BUCKET = os.environ["BUCKET"]

s3 = boto3.client("s3")
logger = Logger()

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    event_body = json.loads(event["body"])
    file_name = event_body["fileName"]
    # human_input = event_body["prompt"]
    human_input = 'Contesta las preguntas, quiero que repondas un solo JSON y nada mas, Ejemplo: { "Aqui pregunta": "Aqui respuesta" }. ¿El documento es sobre una solicitud de devolución de Saldo a Favor?, ¿El documento es sobre un requerimiento?, ¿El documento es sobre el impuesto sobre la renta?, ¿El documento es sobre el impuesto al valor agregado?, ¿El documento es sobre el impuesto sobre prodcucción y servicios?, ¿El documento es sobre retenciones de ISR?, ¿El documento es sobre retenciones de IVA?, ¿A que periodo hace referencia la solicitud de información?, ¿Qué importe está sujeto a aclaración?'

    user = "74d8f4c8-30a1-709b-3c66-2b9a189aca33"
    logger.info("User: %s", user)

    s3.download_file(BUCKET, f"{user}/{file_name}/index.faiss", "/tmp/index.faiss")
    s3.download_file(BUCKET, f"{user}/{file_name}/index.pkl", "/tmp/index.pkl")

    bedrock_runtime = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1",
    )

    embeddings, llm = BedrockEmbeddings(
        model_id="amazon.titan-embed-text-v1",
        client=bedrock_runtime,
        region_name="us-east-1",
    ), BedrockChat(
        model_id="anthropic.claude-3-sonnet-20240229-v1:0", client=bedrock_runtime, region_name="us-east-1"
    )
    faiss_index = FAISS.load_local("/tmp", embeddings, allow_dangerous_deserialization=True)

    retrieval_qa_chat_prompt = hub.pull("langchain-ai/retrieval-qa-chat")
    combine_docs_chain = create_stuff_documents_chain(llm, retrieval_qa_chat_prompt)
    retriever=faiss_index.as_retriever()
    qa = create_retrieval_chain(retriever, combine_docs_chain)

    res = qa.invoke({"input": human_input})
    answer = json.loads(res["answer"])

    response = {
        "success": True,
        "data": answer,
    }

    logger.info(response)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps(response),
    }
