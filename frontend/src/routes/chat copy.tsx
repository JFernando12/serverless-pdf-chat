import React, { useState, useEffect, KeyboardEvent } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { API } from 'aws-amplify';
import { Conversation } from '../common/types';
import ChatSidebar from '../components/ChatSidebar';
import ChatMessages from '../components/ChatMessages';
import LoadingGrid from '../../public/loading-grid.svg';

const Document: React.FC = () => {
  const params = useParams();
  const navigate = useNavigate();

  const [conversation, setConversation] = useState<Conversation | null>(null);
  const [loading, setLoading] = React.useState<string>('idle');
  const [messageStatus, setMessageStatus] = useState<string>('idle');
  const [conversationListStatus, setConversationListStatus] = useState<
    'idle' | 'loading'
  >('idle');
  const [prompt, setPrompt] = useState('');

  const fetchData = async (conversationid = params.conversationid) => {
    setLoading('loading');
    const conversation = await API.get(
      'serverless-pdf-chat',
      `/doc/${params.documentid}/${conversationid}`,
      {}
    );
    setConversation(conversation);
    setLoading('idle');
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handlePromptChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setPrompt(event.target.value);
  };

  const addConversation = async () => {
    setConversationListStatus('loading');
    const newConversation = await API.post(
      'serverless-pdf-chat',
      `/doc/${params.documentid}`,
      {}
    );
    fetchData(newConversation.conversationid);
    navigate(`/doc/${params.documentid}/${newConversation.conversationid}`);
    setConversationListStatus('idle');
  };

  const switchConversation = (e: React.MouseEvent<HTMLButtonElement>) => {
    const targetButton = e.target as HTMLButtonElement;
    navigate(`/doc/${params.documentid}/${targetButton.id}`);
    fetchData(targetButton.id);
  };

  const handleKeyPress = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key == 'Enter') {
      submitMessage();
    }
  };

  const submitMessage = async () => {
    setMessageStatus('loading');

    if (conversation !== null) {
      const previewMessage = {
        type: 'text',
        data: {
          content: prompt,
          additional_kwargs: {},
          example: false,
        },
      };

      const updatedConversation = {
        ...conversation,
        messages: [...conversation.messages, previewMessage],
      };

      setConversation(updatedConversation);
    }

    // await API.post(
    //   "serverless-pdf-chat",
    //   `/${conversation?.document.documentid}/${conversation?.conversationid}`,
    //   {
    //     body: {
    //       fileName: conversation?.document.filename,
    //       prompt: prompt,
    //     },
    //   }
    // );

    // I want to make multiple call in parallel
    const questions = [
      '¿El documento es sobre una solicitud de devolución de Saldo a Favor?',
      '¿El documento es sobre un requerimiento?',
      '¿El documento es sobre el impuesto sobre la renta?',
      '¿El documento es sobre el impuesto al valor agregado?',
      '¿El documento es sobre el impuesto sobre prodcucción y servicios?',
      '¿El documento es sobre retenciones de ISR?',
      '¿El documento es sobre retenciones de IVA?',
      '¿A que periodo hace referencia la solicitud de información?',
      '¿Que importe está sujeto a aclaración?',
    ];

    await Promise.all(
      questions.map(async (question) => {
        await API.post(
          'serverless-pdf-chat',
          `/${conversation?.document.documentid}/${conversation?.conversationid}`,
          {
            body: {
              fileName: conversation?.document.filename,
              prompt: question,
            },
          }
        );
      })
    );

    setPrompt('');
    fetchData(conversation?.conversationid);
    setMessageStatus('idle');
  };

  return (
    <div className="">
      {loading === 'loading' && !conversation && (
        <div className="flex flex-col items-center mt-6">
          <img src={LoadingGrid} width={40} />
        </div>
      )}
      {conversation && (
        <div className="grid grid-cols-12 border border-gray-200 rounded-lg">
          <ChatSidebar
            conversation={conversation}
            params={params}
            addConversation={addConversation}
            switchConversation={switchConversation}
            conversationListStatus={conversationListStatus}
          />
          <ChatMessages
            prompt={prompt}
            conversation={conversation}
            messageStatus={messageStatus}
            submitMessage={submitMessage}
            handleKeyPress={handleKeyPress}
            handlePromptChange={handlePromptChange}
          />
        </div>
      )}
    </div>
  );
};

export default Document;
