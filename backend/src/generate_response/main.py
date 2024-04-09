import os, json
import boto3
from aws_lambda_powertools import Logger
from langchain.llms.bedrock import Bedrock
from langchain_community.chat_message_histories import DynamoDBChatMessageHistory
from langchain.memory import ConversationBufferMemory
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.chains import ConversationalRetrievalChain
from langchain_community.chat_models import BedrockChat

MEMORY_TABLE = os.environ["MEMORY_TABLE"]
BUCKET = os.environ["BUCKET"]


s3 = boto3.client("s3")
logger = Logger()


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    event_body = json.loads(event["body"])
    file_name = event_body["fileName"]
    # human_input = event_body["prompt"]
    human_input = 'Contesta las preguntas y responde en formato json { "${pregunta}": "${respuesta}" }: ¿El documento es sobre una solicitud de devolución de Saldo a Favor?, ¿El documento es sobre un requerimiento?, ¿El documento es sobre el impuesto sobre la renta?, ¿El documento es sobre el impuesto al valor agregado?, ¿El documento es sobre el impuesto sobre prodcucción y servicios?, ¿El documento es sobre retenciones de ISR?, ¿El documento es sobre retenciones de IVA?, ¿A que periodo hace referencia la solicitud de información?, ¿Qué importe está sujeto a aclaración?'
    conversation_id = event["pathParameters"]["conversationid"]

    # user = event["requestContext"]["authorizer"]["claims"]["sub"]
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

    message_history = DynamoDBChatMessageHistory(
        table_name=MEMORY_TABLE, session_id=conversation_id
    )

    memory = ConversationBufferMemory(
        memory_key="chat_history",
        chat_memory=message_history,
        input_key="question",
        output_key="answer",
        return_messages=True,
    )

    qa = ConversationalRetrievalChain.from_llm(
        llm=llm,
        retriever=faiss_index.as_retriever(),
        memory=memory,
        return_source_documents=True,
    )

    res = qa.invoke({"question": human_input})

    logger.info(res)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": res["answer"],
    }
