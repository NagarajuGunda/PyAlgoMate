import asyncio
import threading
from telegram import Bot


class TelegramBot:
    def __init__(self, botToken, channelId):
        self.bot = Bot(token=botToken)
        self.channelId = channelId
        self.messageQueue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()
        self.sendThread = threading.Thread(target=self._runLoop)
        self.sendThread.daemon = True
        self.readyEvent = threading.Event()
        self.stopEvent = threading.Event()
        self.sendThread.start()

    def sendMessage(self, message):
        self.readyEvent.wait()
        self.loop.call_soon_threadsafe(self._pushMessage, message)

    def _pushMessage(self, message):
        asyncio.run_coroutine_threadsafe(
            self.messageQueue.put(message), self.loop)

    def _runLoop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._sendMessages())

    async def _sendMessages(self):
        self.readyEvent.set()
        while True:
            message = await self.messageQueue.get()
            await asyncio.sleep(4)  # Add a delay between each message send
            await self._safeSend(message)
            self.messageQueue.task_done()

            if self.stopEvent.is_set() and self.messageQueue.empty():
                break

    async def _safeSend(self, message):
        try:
            await self.bot.send_message(chat_id=self.channelId, text=message)
        except Exception as e:
            print(f"Error sending message: {str(e)}")

    def stop(self):
        self.stopEvent.set()

    def delete(self):
        self.stop()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.sendThread.join()

    def waitUntilFinished(self):
        self.sendThread.join()


if __name__ == "__main__":
    bot = TelegramBot("botid", "-chatid")
    bot.sendMessage("Hello, world!")
    bot.stop()
    bot.waitUntilFinished()
    bot.delete()
    print("All messages sent. TelegramBot instance deleted. Exiting the process.")
