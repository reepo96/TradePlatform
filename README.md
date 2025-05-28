# TradePlatform
China Futures Trading Platform
  This platform utilizes the AlgoPlus package to provide a further encapsulated interface for trading execution and various information queries in China's futures market. Platform users can independently develop trading strategies using Python. The strategy code must implement the def OnBar(level) function, which serves as the entry point for the platform to invoke the strategy. At each registered K-line generation event, the platform will call the OnBar function once to execute the strategy.
  The strategy code file is placed in the "strategy" directory, just like the several strategy examples currently stored in the directory
