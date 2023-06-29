from telegram import Bot
import threading
import queue
import asyncio


class TelegramBot:
    def __init__(self, botToken, channelId):
        self.bot = Bot(token=botToken)
        self.channelId = channelId
        self.messageQueue = queue.Queue()
        self.sendThread = threading.Thread(target=self._sendMessages)
        self.sendThread.daemon = True  # Set the thread as daemon
        self.sendThread.start()

    def _sendMessages(self):
        async def sendMessageAsync(message):
            await self.bot.send_message(
                chat_id=self.channelId, text=message)

        while True:
            message = self.messageQueue.get()
            asyncio.run(sendMessageAsync(message))
            self.messageQueue.task_done()

    def sendMessage(self, message):
        self.messageQueue.put(message)

    def __del__(self):
        pass  # No need to join the thread explicitly
