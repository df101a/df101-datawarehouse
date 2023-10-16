# DF101: Data-Driven DAO

Welcome to the official GitHub repository of **DF101**, a Decentralized Autonomous Organization (DAO) that leverages data as a primary means to drive decisions and actions in our decentralized fund.

## Overview

In the rapidly evolving landscape of decentralized finance, the role of accurate and reliable data is paramount. DF101 believes that data should be transparent, open, and be at the core of every financial decision made in the DeFi space. Our aim is to establish a benchmark for data transparency and utilization in DeFi.

## Why DF101?

1. **Transparency**: In an industry that values decentralization and transparency, we ensure that every data integration is open-sourced, providing a clear view of its origins and methodologies.
2. **Community-Driven**: We encourage developers, data scientists, and DeFi enthusiasts to contribute, ensuring a diverse and comprehensive set of data integrations.
3. **Innovation in DeFi**: With data at the helm, we aim to inspire and lead groundbreaking developments in the decentralized finance sector.

## Contribute

We're always looking for contributions to improve our data integrations and drive innovation:

1. **Fork** the repo.
2. **Clone** your forked repo.
3. **Commit** your changes.
4. **Push** to your fork.
5. Submit a **Pull Request** to the main `df101` repository.

For detailed steps on contributing, check our [CONTRIBUTING.md](CONTRIBUTING.md).

## Getting Started

To start using our data integrations:

1. Explore our integrations to find datasets and tools.
2. Follow the instructions provided in individual integration folders.

## Feedback and Discussions

We value community feedback. If you have suggestions or find issues:

- Raise an [issue](https://github.com/df101/df101/issues).
- Join our [Discord](#) or [Telegram](#) (replace `#` with actual links).

## License

Our work is open-sourced under the [MIT License](LICENSE). We believe in the freedom to use, change, and distribute our work to foster development in DeFi.

---

# Integrations

Currently we have the folloiwng integrations:

| Attribute | Entity | Description | Data Source |
| --- | --- | --- | --- |
| Capitalization | Market Data | Current price * Circulation supply | DWH |
| Trading volume | Market Data |  | CG, CMC |
| Assets staked on chain (TVL) | Market Data | Current price * Max supply | DefiLama |
| Market capitalisation fully diluted | Market Data | Current price * Circulation supply / TVL | DWH |
| Current price | Market Data |  | CG, CMC |
| circulating_supply_percentage | Market Data |  | Messari |
| tokens_staked | Market Data |  | Messari |
| assets_staked_on_chain | Market Data |  | Messari |
| drop_from_ath | Market Data |  | Glassnode |
| active_validators_count  | Market Data |  | Glassnode |
|  inflation_rate  | Market Data |  | Glassnode |
|  wallet_count | Market Data |  | Glassnode |
|  gas_price_mean | Market Data |  | Glassnode |
| Drop from ATH | Market Data |  |  |
| Max supply | Tokenomics |  |  |
| Total supply | Tokenomics |  |  |
| Circulation supply % | Tokenomics |  |  |
| Twitter | Social |  | Twitter |
| Telegram | Social |  | Telegram |
| Reddit | Social |  | Reddit |
| Github commits | Developers |  | GitHub |
| Average commits per week | Developers |  | GitHub |
| Github contributors | Developers |  | GitHub |
| Github forks | Developers |  | GitHub |
| Github watchers | Developers |  | GitHub |
| Github stars | Developers |  | GitHub |
| Merged pull requests | Developers |  | GitHub |
| Open issues | Developers |  | GitHub |
| Closed issues | Developers |  | GitHub |
| Commit speed per contributor |  | Commit speed / Github contributors | DWH |
