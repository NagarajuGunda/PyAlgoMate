import logging
import asyncio
import threading
import signal
from typing import List, Dict

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
from telegram import ReplyKeyboardMarkup, Update, Bot
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

reply_keyboard = [
    ["Get PnL"],
    ["Exit All Positions"],
    ["Done"],
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
    def __init__(self, botToken, channelId, allowedUserIds):
        self.botToken = botToken
        self.bot = Bot(token=botToken)
        self.channelId = channelId
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
        while True:
            message = await self.messageQueue.get()
            await asyncio.sleep(4)  # Add a delay between each message send
            await self._safeSend(message)
            self.messageQueue.task_done()

            if self.stopEvent.is_set():
                break

    async def _safeSend(self, message):
        try:
            await self.bot.send_message(chat_id=self.channelId, text=message)
        except Exception as e:
            logger.info(f"Error sending message: {str(e)}")

    def stop(self):
        async def stopPolling(updater):
            await updater.stop()

        asyncio.run_coroutine_threadsafe(
            stopPolling(self.application.updater), self.loop)
        self.stopEvent.set()

    def delete(self):
        self.stop()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.sendThread.join()

    def waitUntilFinished(self):
        self.sendThread.join()

    async def choice_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle user choices, including "Get PnL" and "Exit All Positions"."""
        text = update.message.text
        if text == "Get PnL":
            return await self.select_strategy(update, context, "get_pnl")
        elif text == "Exit All Positions":
            return await self.select_strategy(update, context, "exit_all_positions")
        else:
            return await self.done(update, context)

    async def select_strategy(self, update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> int:
        strategy_names = [[strategy.strategyName]
                          for strategy in self.strategies]
        keyboard = ReplyKeyboardMarkup(strategy_names, one_time_keyboard=True)
        await update.message.reply_text("Please select a strategy:", reply_markup=keyboard)
        context.user_data["selected_action"] = action
        return SELECT_STRATEGY

    async def strategy_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        selected_strategy = update.message.text
        action = context.user_data.get("selected_action")

        strategy = next(
            (s for s in self.strategies if s.strategyName == selected_strategy), None)

        if not strategy:
            return await self.unexpected_message_handler(update, context)

        # Now you have both the selected strategy and the action to perform
        if action == "get_pnl":
            pnl_message = f"PnL for {selected_strategy}: {strategy.getOverallPnL()}"
            await update.message.reply_text(pnl_message, parse_mode="markdown")
        elif action == "exit_all_positions":
            # strategy.exitAllPositions()
            exit_message = f"Exiting all positions for {selected_strategy}..."
            await update.message.reply_text(exit_message)

        return await self.start(update, context)

    async def invalid_strategy_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text("Invalid strategy selection. Please choose a valid strategy.")
        return SELECT_STRATEGY

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        user_id = update.message.from_user.id

        # Check if the user ID is in the list of allowed user IDs
        if user_id not in self.allowedUserIds:
            await update.message.reply_text(
                "Sorry, you are not authorized to access this bot."
            )
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
        self.application = Application.builder().token(self.botToken).build()

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
    bot = TelegramBot("botid", "-chatid")
    bot.sendMessage("Hello, world!")

    def handle_interrupt(signum, frame):
        logger.info("Ctrl+C received. Stopping the bot...")
        bot.stop()
        bot.waitUntilFinished()
        bot.delete()
        logger.info("Bot stopped. Exiting the process.")
        exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)
    bot.waitUntilFinished()


if __name__ == "__main__":
    main()
