import polars as pl
from datetime import datetime
from pydantic import BaseModel

class Stock(BaseModel):
    ticker: str
    name: str
    market_value: float
    suggested_buy_price: float = None
    suggested_sell_price: float = None
    suggested_stop_loss: float = None

    def update_market_value(self, new_value):
        self.market_value = new_value
    
    def update_suggestions(self, buy_price,sell_price,stop_loss):
        if buy_price <= 0 or sell_price <= 0 or stop_loss <= 0:
            raise ValueError("Values must be non-negative.")
        if buy_price < sell_price:
            raise ValueError("Buy price is higher than sell price, not possible.")
        if stop_loss >= self.market_value:
            raise ValueError("Stoploss is equal or higher than present market value, not possible.")
        

        self.suggested_buy_price  = buy_price
        self.suggested_sell_price = sell_price
        self.suggested_stop_loss  = stop_loss
        


class Position(BaseModel):
    stock: Stock
    quantity: int
    purchase_price: float

    def current_price(self):
        return self.stock.market_value

    def calculate_profit_loss(self):
        retrieved_current_price = self.current_price()
        if retrieved_current_price is None:
            return 0
        return (self.retrieved_current_price - self.buy_price) * self.quantity




class Portfolio(BaseModel):
    name: str
    id: int
    investor_id: int
    positions: list[Position]

    def add_position(self, position):
        self.positions.append(position)

    def get_total_value(self):
        total_value = 0
        for position in self.positions:
            total_value += position.quantity * position.current_price
        return total_value

    def get_position_by_id(self, position_id):
        for position in self.positions:
            if position.id == position_id:
                return position


class Investor(BaseModel):
    name: str
    id: int
    cash_balance: float
    portfolios: list[Portfolio] = []

    def add_portfolio(self, portfolio: Portfolio):
        self.portfolios.append(portfolio)

    def add_position_to_portfolio(self, portfolio_id, stock, quantity, purchase_price):
        for portfolio in self.portfolios:
            if portfolio.id == portfolio_id:
                portfolio.add_position(Position(stock, quantity, purchase_price))

    def get_portfolio_value(self, portfolio_id: int) -> float:
        for portfolio in self.portfolios:
            if portfolio.id == portfolio_id:
                return portfolio.get_total_value()

    def get_net_worth(self) -> float:
        total_worth = self.cash_balance
        for portfolio in self.portfolios:
            total_worth += portfolio.get_total_value()
        return total_worth


class Market_data_provider(BaseModel):
    date_retrieved: datetime
    market_data_from_source: dict
    historical_data_store: dict[str,pl.DataFrame] #I will have to create a data structure for Pydantic

class Risk_assessor(BaseModel):
    type_of_risk = str
    level_of_risk = int # Maybe 1 to 100? Where 100 is pure recklessnes?


class Bank(BaseModel):
    market_data_provider: Market_data_provider  # Assuming a custom type or interface
    risk_assessment_model: Risk_assessor
    stock_data: dict[str, Stock]
    portfolio_data: dict[int, Portfolio] #what is this?
    investor_data: dict[int, Investor]

    # ... (rest of Bank methods)

    def get_stock_by_ticker(self, ticker):
        if ticker in self.stock_data:
            return self.stock_data[ticker]
        else:
            return None

    def update_market_data(self):
        # Retrieve real-time market data from the provider
        new_data = self.market_data_provider.get_data()

        # Update stock data and store it locally
        for row in new_data.itertuples():
            ticker = row.ticker
            if ticker not in self.stock_data:
                self.stock_data[ticker] = Stock(ticker, row.name, row.market_value)
            else:
                self.stock_data[ticker].update_market_value(row.market_value)


    def assess_risk(self, stock_id):
        # Implement risk assessment model
        risk_assessment_score = 1
        return risk_assessment_score

    def recommend_buy_sell_stoploss(self, stock_id):
        # Implement recommendation logic based on risk assessment and other factors
        buy_recommendation = 0
        sell_recommendation = 0
        stop_loss_recommendation = 0
        return buy_recommendation, sell_recommendation, stop_loss_recommendation

    def create_portfolio(self, investor_id, portfolio_name):
        # Create a new portfolio and add it to the investor's list
        portfolio = Portfolio(
            name=portfolio_name, 
            id=1,
            investor_id=investor_id,
            positions=[])
        self.portfolio_data[portfolio.id] = portfolio
        self.investor_data[investor_id].add_portfolio(portfolio)

    def add_position_to_portfolio(self, portfolio_id, stock_id, quantity, buy_price):
        # Add a position to the specified portfolio
        portfolio = self.portfolio_data[portfolio_id]
        portfolio.add_position(Position(stock_id, quantity, buy_price))

    def get_portfolio_performance(self, portfolio_id, start_date, end_date):
        # Retrieve historical data for the portfolio's positions
        historical_data = []
        for position in self.portfolio_data[portfolio_id].positions:
            stock_data = self.historical_data_store.get_data(position.stock_id, start_date, end_date)
            historical_data.append(stock_data)

        # Calculate portfolio performance based on the historical data
        # ...

    def save_data(self):
        # Save stock data, portfolio data, and investor data to a persistent storage
        # ...
        all_went_well = 0 
        if all_went_well == 1:
            return "Ok"
        else: return "NOk"
    

    def load_data():
        # Load stock data, portfolio data, and investor data from a persistent storage
        # ...
        all_went_well = 0 
        if all_went_well == 1:
            return "Ok"
        else: return "NOk"
    