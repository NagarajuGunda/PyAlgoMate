import logging
import asyncio
import threading
import signal
import pandas as pd
import numpy as np
from typing import List, Dict
import matplotlib.pyplot as plt
from pandas.plotting import table
from pyalgomate.core import State
import io
import datetime
import time
from telegram.error import RetryAfter
from telegram import __version__ as TG_VER

try:
    from telegram import __version_info__
except ImportError:
    __version_info__ = (0, 0, 0, 0, 0)  # type: ignore[assignment]

if __version_info__ < (20, 0, 0, "alpha", 1):
    raise RuntimeError(
        f"This example is not compatible with your current PTB version {TG_VER}. To view the "
        f"{TG_VER} version of this example, "
        f"visit https://docs.python-telegram-bot.org/en/v{TG_VER}/examples.html"
    )
from telegram import ReplyKeyboardMarkup, Update, Bot, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)

from telegram.ext.filters import BaseFilter

# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

CHOOSING, SELECT_STRATEGY, TYPING_REPLY = range(3)

GET_STATUS = "Get Status"
GET_PNL_CHART = "Get PnL Charts"
GET_TRADE_BOOK = "Get Trade Book"
EXIT_ALL_POSITIONS = "Exit All Positions"
DONE = "Done"

reply_keyboard = [
    [GET_STATUS],
    [GET_PNL_CHART],
    [GET_TRADE_BOOK],
    [EXIT_ALL_POSITIONS],
    [DONE],
]
markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True)


class ValidStrategyFilter(BaseFilter):
    def __init__(self, strategies):
        self.strategies = strategies

    def filter(self, message):
        return any(strategy.strategyName == message.text for strategy in self.strategies)


class InvalidStrategyFilter(BaseFilter):
    def __init__(self, strategies):
        self.strategies = strategies

    def filter(self, message):
        return all(strategy.strategyName != message.text for strategy in self.strategies)


class TelegramBot:
    def __init__(self, botToken, channelId, allowedUserIds=[]):
        self.botToken = botToken
        self.bot = Bot(token=botToken)
        self.channelId = channelId

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            if str(e).startswith('There is no current event loop in thread'):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise

        self.messageQueue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()
        self.sendThread = threading.Thread(target=self._runLoop)
        self.sendThread.daemon = True
        self.readyEvent = threading.Event()
        self.stopEvent = threading.Event()
        self.sendThread.start()
        self.application = None
        self.allowedUserIds = allowedUserIds

        self.strategies: List[object] = []

        self.last_message_time = 0
        self.original_sleep_interval = 0.5
        self.current_sleep_interval = self.original_sleep_interval

    def addStrategy(self, strategy):
        self.strategies.append(strategy)

    def sendMessage(self, message):
        self.readyEvent.wait()
        self.loop.call_soon_threadsafe(self._pushMessage, message)

    def _pushMessage(self, message):
        asyncio.run_coroutine_threadsafe(
            self.messageQueue.put(message), self.loop)

    def _runLoop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(
            asyncio.gather(self._sendMessages(), self.run()))

    async def _sendMessages(self):
        self.readyEvent.set()
        failed_messages = asyncio.Queue()

        while True:
            try:
                try:
                    if not failed_messages.empty():
                        message = await asyncio.wait_for(failed_messages.get(), timeout=1)
                    else:
                        message = await asyncio.wait_for(self.messageQueue.get(), timeout=1)
                except asyncio.TimeoutError:
                    if self.stopEvent.is_set():
                        break
                    else:
                        continue

                # Calculate time elapsed since the last message
                elapsed_time = time.time() - self.last_message_time

                if elapsed_time < self.current_sleep_interval:
                    # Sleep for the remaining time to respect the current interval
                    await asyncio.sleep(self.current_sleep_interval - elapsed_time)

                if type(message) is dict:
                    content = message['message']
                    channelId = self.channelId
                    if 'channelId' in message and message['channelId'] is not None:
                        channelId = message['channelId']
                    messageThreadId = None
                    if 'messageThreadId' in message:
                        messageThreadId = message['messageThreadId']
                    if isinstance(content, str):
                        await self.bot.send_message(chat_id=channelId, text=content, message_thread_id=messageThreadId)
                    else:
                        await self.bot.send_photo(chat_id=channelId, photo=content, message_thread_id=messageThreadId)
                else:
                    await self.bot.send_message(chat_id=self.channelId, text=message)
                self.messageQueue.task_done()

                self.last_message_time = time.time()

                # Reset the sleep interval to the original value
                self.current_sleep_interval = self.original_sleep_interval

            except RetryAfter as e:
                retry_after_seconds = e.retry_after
                self.current_sleep_interval = max(
                    retry_after_seconds, self.current_sleep_interval)
                logger.warning(
                    f"Rate limit exceeded. Sleeping for {retry_after_seconds}. Error: {e}")
                await asyncio.sleep(retry_after_seconds)
                await failed_messages.put(message)

            except Exception as e:
                logger.exception(f"Failed to send message: {e}")
                await failed_messages.put(message)

            if self.stopEvent.is_set():
                break

    def stop(self):
        async def stopPolling(updater):
            await updater.stop()

        self.stopEvent.set()

        asyncio.run_coroutine_threadsafe(
            stopPolling(self.application.updater), self.loop)

    def delete(self):
        self.stop()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.sendThread.join()

    def waitUntilFinished(self):
        self.sendThread.join()

    async def get_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        strategiesDetails = [{
            "name": strategy.strategyName,
            "pnl": strategy.getOverallPnL(),
            "open": len(strategy.getActivePositions()),
            "closed": len(strategy.getClosedPositions()),
            "running": not (strategy.state == State.UNKNOWN or strategy.state == State.EXITED)
        } for strategy in self.strategies]

        message = ""

        for details in strategiesDetails:
            message += f"{'🔴' if details['pnl'] < 0 else '🟢'} {'<s>' if not details['running'] else ''}<a href='https://github.com/NagarajuGunda/PyAlgoMate/'>{details['name']}</a>{'</s>' if not details['running'] else ''}  <b>₹ {details['pnl']:.2f}</b>\n"
            message += f"<i>Open = {details['open']} Closed = {details['closed']} Total = {details['open'] + details['closed']}</i>\n\n"

        overallPnL = sum([details['pnl'] for details in strategiesDetails])
        message += f"\n"
        message += f"{'🔴' if overallPnL < 0 else '🟢'} Overall PNL <b>•  ₹ {overallPnL:.2f}</b>\n\n"

        if len(self.strategies):
            feedAlive = self.strategies[0].getFeed().isDataFeedAlive()

            message += f"<i>Data Feed:</i> {'🔵' if feedAlive else '⭕'}"

        await update.message.reply_text(message, parse_mode='HTML', disable_web_page_preview=True)

        return await self.start(update, context)

    async def choice_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self.isUserAllowed(update):
            return ConversationHandler.END

        text = update.message.text

        if text == GET_STATUS:
            return await self.get_status(update, context)

        return await self.select_strategy(update, context, text)

    async def select_strategy(self, update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> int:
        strategy_names = [[strategy.strategyName]
                          for strategy in self.strategies]
        keyboard = ReplyKeyboardMarkup(strategy_names, one_time_keyboard=True)
        await update.message.reply_text("Please select a strategy:", reply_markup=keyboard)
        context.user_data["selected_action"] = action
        return SELECT_STRATEGY

    async def strategy_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self.isUserAllowed(update):
            return ConversationHandler.END

        selected_strategy = update.message.text
        action = context.user_data.get("selected_action")

        strategy = next(
            (s for s in self.strategies if s.strategyName == selected_strategy), None)

        if not strategy:
            return await self.unexpected_message_handler(update, context)

        # Now you have both the selected strategy and the action to perform
        if action == GET_PNL_CHART:
            await update.message.reply_photo(photo=strategy.getPnLImage())
        elif action == GET_TRADE_BOOK:
            try:
                tradesDf = strategy.getTrades()
                tradesDf = tradesDf.loc[pd.to_datetime(
                    tradesDf['Entry Date/Time'], format='%Y-%m-%d %H:%M:%S').dt.date == datetime.date.today()]

                if tradesDf.shape[0] == 0:
                    await update.message.reply_text(f'There are no trades for today yet!')
                else:
                    ax = plt.subplot(111, frame_on=False)
                    ax.xaxis.set_visible(False)
                    ax.yaxis.set_visible(False)
                    tab = table(ax, tradesDf, loc='center', cellLoc='center')
                    tabFigure = tab.get_figure()
                    imageBuffer = io.BytesIO()
                    tabFigure.savefig(imageBuffer, format='png',
                                      bbox_inches='tight')
                    imageBuffer.seek(0)

                    await update.message.reply_photo(photo=InputFile(imageBuffer))
            except Exception as e:
                await update.message.reply_text(f'Exception occured while sending trade book. Error: {e}')
        elif action == EXIT_ALL_POSITIONS:
            strategy.closeAllPositions()
            exit_message = f"Exiting all positions for {selected_strategy}..."
            await update.message.reply_text(exit_message)
        else:
            await update.message.reply_text('Invalid strategy action')

        return await self.start(update, context)

    async def invalid_strategy_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self.isUserAllowed(update):
            return ConversationHandler.END

        await update.message.reply_text("Invalid strategy selection. Please choose a valid strategy.")
        return SELECT_STRATEGY

    async def isUserAllowed(self, update: Update):
        user_id = update.message.from_user.id
        if user_id not in self.allowedUserIds:
            await update.message.reply_text(
                "Sorry, you are not authorized to access this bot."
            )
            return False
        return True

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await self.isUserAllowed(update):
            return ConversationHandler.END

        """Start the conversation and ask user for input."""
        await update.message.reply_text(
            "Hi! I am PyAlgoMate Botter. How can I help you today?",
            reply_markup=markup,
        )

        return CHOOSING

    async def done(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        user_data = context.user_data
        user_data.clear()
        return ConversationHandler.END

    async def unexpected_message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text("Oops! Something went wrong. Returning to the main menu.")
        return await self.start(update, context)

    async def run(self):
        """Run the bot."""
        # Create the Application and pass it your bot's token.
        self.application = Application.builder().token(
            self.botToken).read_timeout(30).write_timeout(30).build()

        # Add conversation handler with the states CHOOSING, SELECT_STRATEGY
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", self.start)],
            states={
                CHOOSING: [
                    MessageHandler(
                        filters.TEXT & ~(filters.COMMAND |
                                         filters.Regex("^Done$")),
                        self.choice_handler
                    ),
                ],
                SELECT_STRATEGY: [
                    MessageHandler(
                        ValidStrategyFilter(self.strategies),
                        self.strategy_action
                    ),
                    MessageHandler(
                        InvalidStrategyFilter(self.strategies),
                        self.invalid_strategy_selection
                    ),
                ],
            },
            fallbacks=[MessageHandler(filters.Regex("^Done$"), self.done)],
        )

        self.application.add_handler(conv_handler)

        await self.application.initialize()  # inits bot, update, persistence
        await self.application.start()
        await self.application.updater.start_polling()


def main() -> None:
    bot = TelegramBot("botId", "channelId")
    bot.sendMessage({"message": "Hello, world1!", "messageThreadId": "2"})

    def handle_interrupt(signum, frame):
        logger.info("Ctrl+C received. Stopping the bot...")
        bot.stop()
        bot.waitUntilFinished()
        logger.info("Bot stopped. Exiting the process.")
        exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)
    bot.waitUntilFinished()


if __name__ == "__main__":
    main()
