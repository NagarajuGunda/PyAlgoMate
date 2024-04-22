# PyAlgoMate
[![Python package](https://github.com/NagarajuGunda/PyAlgoMate/actions/workflows/python-package.yml/badge.svg)](https://github.com/NagarajuGunda/PyAlgoMate/actions/workflows/python-package.yml)

PyAlgoMate is a Python library for event-driven algorithmic trading, developed as an extension of PyAlgoTrade (https://github.com/gbeced/pyalgotrade).

With PyAlgoMate, you can seamlessly perform backtesting, paper trading, and live trading with popular brokers such as Finvasia and Zerodha. It offers a comprehensive set of tools and functionalities to support various stages of the trading process.

## Getting Started

To get started with PyAlgoMate, follow the steps below:

### 1. Clone/Download the Repository

First, clone or download the PyAlgoMate repository to your local machine. You can do this by clicking on the "Code" button on the repository's GitHub page and selecting your preferred method (e.g., cloning with Git or downloading as a ZIP file).

```shell
git clone https://github.com/NagarajuGunda/PyAlgoMate.git
```


### 2. Set Up a Python Virtual Environment

It is highly recommended to create a Python virtual environment to isolate PyAlgoMate's dependencies from your system's Python installation. This ensures that the library and its dependencies do not conflict with other Python projects on your machine.

Navigate to the directory of the cloned/downloaded repository using the command line and create a new virtual environment.

```shell
cd PyAlgoMate
python3 -m venv .venv
```

### 3. Activate the Virtual Environment

Activate the virtual environment using the appropriate command for your operating system.

#### On Linux
```shell
source .venv/bin/activate
```
#### On Windows
```shell
.venv\Scripts\activate
```

### 4. Install the Dependencies

Once the virtual environment is activated, you can install the required dependencies for PyAlgoMate using pip.

```shell
pip3 install -r requirements.txt
```

This command will install all the necessary packages specified in the requirements.txt file.

Congratulations! You have successfully set up PyAlgoMate and installed all the necessary dependencies. Now you're fully equipped to explore the wide range of features offered by PyAlgoMate. It is designed to assist you in writing effective trading strategies, conducting thorough backtesting, and executing trades. PyAlgoMate provides a powerful framework to develop and implement your own trading strategies in Python. Let's dive in and leverage the capabilities of PyAlgoMate to enhance your trading experience!


## PyAlgoMate Usage

### Running a Backtest

To perform a backtest using PyAlgoMate, it is recommended to use the command-line interface (CLI) method. Prior to executing the command, users need to set an environment variable called `PYTHONPATH` to ensure Python can locate the PyAlgoMate module. Follow the instructions below based on the operating system:

#### On Unix/Linux
```shell
export PYTHONPATH=path_to_pyalgomate_directory
```
#### On Windows
```shell
set PYTHONPATH=path_to_pyalgomate_directory
```

After setting the `PYTHONPATH` variable, users can run a strategy file with CLI capability using the following command:

```
python pyalgomate/strategies/strategy.py backtest --data "path_to_parquet_file" --underlying BANKNIFTY
```

Replace `path_to_parquet_file` with the actual path to the Parquet file containing the relevant data. Alternatively, you can find sample data under the repository [NSEIndexOptionsData](https://github.com/NagarajuGunda/NSEIndexOptionsData).

Specify the `--underlying` parameter with the appropriate underlying asset for the backtest.

To explore the available options and parameters supported by the CLI, use the `--help` flag with the strategy file, as shown below:

```
python pyalgomate/strategies/strategy.py --help
python pyalgomate/strategies/strategy.py backtest --help
```

These commands will provide information on the available options and their usage.

Please note that the README will be updated as the project progresses, including additional instructions and improvements. We apologize for any inconvenience caused and appreciate your understanding as we enhance the usability of the library.

For further assistance or any questions, please reach out. Happy exploring and trading with PyAlgoMate!

## Issues
### On Windows
If you are having trouble with MTM graph generation on Windows 11 try to downgrade `kaleido` library from latest to 0.1.0
```bash
pip install --upgrade "kaleido==0.1.*"
```

## Analysing back test results
All backtest results are stored in csv file with file name as a strategy name. You can analyise the results with backtest analalyzer using below command
```python
streamlit run backtestanalyzer.py
```
## Contributing

If you find any issues or have suggestions for improvements, contributions to PyAlgoMate are welcome. Please open a GitHub issue or submit a pull request with your proposed changes.
