import logging
import os
import time
from typing import List, Tuple, Set
from bot.bytedance.coze_session import CozeSession
from pathlib import Path
from cozepy import Coze, TokenAuth, Message, File, MessageContentType, MessageRole, MessageObjectString, \
    MessageObjectStringType


class CozeClient(object):
    def __init__(self, coze_api_key, base_url: str):
        self.coze_api_key = coze_api_key
        self.base_url = base_url
        self.coze = Coze(base_url=base_url,
                         auth=TokenAuth(token=coze_api_key))

    def file_upload(self, path: str) -> File:
        return self.coze.files.upload(file=Path(path))

    def _send_chat(self, bot_id: str,
                   user_id: str, additional_messages: List[Message], session: CozeSession):
        conversation_id = None
        # 查看目前回话信息是否过多
        session.count_user_message()
        if session.get_conversation_id() is not None:
            conversation_id = session.get_conversation_id()
            # 如果session信息没有超出上限，加入至额外信息
            for message in session.messages:
                additional_messages.insert(
                    0,
                    Message.build_assistant_answer(message["content"])
                    if message.get("role") == "assistant"
                    else Message.build_user_question_text(message["content"]),
                )

        # 对 additional_messages进行去重 与 严格按照时间顺序排序(从旧到新排序)
        # 参考 https://www.coze.com/open/docs/developer_guides/chat_v3
        additional_messages = self.deduplicate_messages(additional_messages)  # 去重
        additional_messages = self.sort_messages_by_timestamp(additional_messages) # 排序

        chat_poll = self.coze.chat.create_and_poll(
            bot_id=bot_id,
            user_id=user_id,
            conversation_id=conversation_id,
            additional_messages=additional_messages
        )
        # ChatPoll 在类型chat里面存储了conversation_id 参考:cozepy.Chat (init.py line 255 )
        chat_info = chat_poll.chat
        session.set_conversation_id(chat_info.conversation_id)
        message_list = chat_poll.messages
        for message in message_list:
            logging.debug('got message:', message.content)
        return message_list

    def create_chat_message(self, bot_id: str, query: str, additional_messages: List[Message], session: CozeSession):
        if additional_messages is None:
            additional_messages = [Message.build_user_question_text(query)]
        else:
            additional_messages.append(Message.build_user_question_text(query))
        return self._send_chat(bot_id, session.get_user_id(), additional_messages, session)

    def create_message(self, file: File) -> Message:

        message_object_string = None
        if self.is_image(file.file_name):
            message_object_string = MessageObjectString.build_image(file.id)
        else:
            message_object_string = MessageObjectString.build_file(file.id)
        return Message.build_user_question_objects([message_object_string])

    def is_image(self, filepath: str):
        valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']
        extension = os.path.splitext(filepath)[1].lower()
        return extension in valid_extensions

    def deduplicate_messages(self,messages: List[Message]) -> List[Message]:
        """
        对 Message 列表进行去重。

        基于消息的角色、类型、内容和内容类型判断消息是否重复。

        Args:
            messages: 待去重的 Message 列表。

        Returns:
            去重后的 Message 列表。
        """
        seen_messages: Set[Tuple] = set()
        unique_messages: List[Message] = []
        for message in messages:
            message_tuple: Tuple = (message.role, message.type, message.content, message.content_type)
            if message_tuple not in seen_messages:
                unique_messages.append(message)
                seen_messages.add(message_tuple)
        return unique_messages

    def sort_messages_by_timestamp(self,messages: List[Message]) -> List[Message]:
        """
        对 Message 列表按照时间戳进行排序 (从旧到新)。

        Args:
            messages: 待排序的 Message 列表。

        Returns:
            按照时间戳排序后的 Message 列表 (从旧到新)。
        """
        def get_message_timestamp(message: Message) -> int:
            return message.created_at if message.created_at is not None else 0

        messages.sort(key=get_message_timestamp)
        return messages