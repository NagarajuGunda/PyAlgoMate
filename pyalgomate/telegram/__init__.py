import asyncio
import threading
from telegram import Bot


class TelegramBot:
    def __init__(self, botToken, channelId):
        self.bot = Bot(token=botToken)
        self.channelId = channelId
        self.messageQueue = asyncio.Queue()
        self.loop = None
        self.sendThread = threading.Thread(target=self._runLoop)
        self.sendThread.daemon = True
        # Event to signal when the worker thread is ready
        self.readyEvent = threading.Event()
        self.stopEvent = threading.Event()  # Event to signal the stop signal
        self.sendThread.start()

    def sendMessage(self, message):
        self.readyEvent.wait()  # Wait until the worker thread is ready
        asyncio.run_coroutine_threadsafe(
            self.messageQueue.put(message), self.loop)

    def _runLoop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._sendMessages())

    async def _sendMessages(self):
        self.readyEvent.set()  # Signal that the worker thread is ready
        while True:
            message = await self.messageQueue.get()
            # Add a delay of 1 second between each message send
            await asyncio.sleep(1)
            try:
                await self._safeSend(message)
            except Exception as e:
                print(f"Error sending message: {str(e)}")
            self.messageQueue.task_done()

            if self.stopEvent.is_set() and self.messageQueue.empty():
                break  # Exit the loop if stop signal is received and queue is empty

    async def _safeSend(self, message):
        try:
            await self.bot.send_message(chat_id=self.channelId, text=message)
        except Exception as e:
            print(f"Error sending message: {str(e)}")

    def stop(self):
        self.stopEvent.set()  # Set the stop signal

    def delete(self):
        self.stop()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.sendThread.join()

    def waitUntilFinished(self):
        self.sendThread.join()


if __name__ == "__main__":
    bot = TelegramBot(
        "botid", "-chatid")
    bot.sendMessage("Hello, world!")
    bot.stop()  # Signal the stop event
    bot.waitUntilFinished()
    bot.delete()  # Delete the TelegramBot instance
    print("All messages sent. TelegramBot instance deleted. Exiting the process.")
