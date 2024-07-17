- Wrote unit tests for each tx type, cumulatively.
- Kept clients in memory for simplicity, but design allows for easy migration to a db. Uses a getter/setter pattern to allow for async db updates. Can refactor to batch updates to reduce db calls.
- Assumed empty amount field for dispute, resolve, and chargeback types.