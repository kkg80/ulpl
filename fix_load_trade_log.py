with open("ulpl_prepecity.py", "r") as f:
    content = f.read()

new_load_trade_log = """def load_trade_log() -> pd.DataFrame:
    if os.path.exists(TRADE_LOG_PATH):
        try:
            df = pd.read_excel(TRADE_LOG_PATH)
            # Add missing columns
            for col, dtype in TRADE_LOG_DTYPES.items():
                if col not in df:
                    df[col] = pd.Series(dtype=dtype)
                elif df[col].dtype != dtype:
                    df[col] = df[col].astype(dtype, errors="ignore")
            # Drop old columns not in TRADE_LOG_COLUMNS
            df = df[[c for c in TRADE_LOG_COLUMNS if c in df]]
            return df
        except Exception as e:
            logger.error(f"Failed to read trade log: {e}")

    df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in TRADE_LOG_DTYPES.items()})
    df.to_excel(TRADE_LOG_PATH, index=False)
    return df"""

old_load_trade_log = """def load_trade_log() -> pd.DataFrame:
    if os.path.exists(TRADE_LOG_PATH):
        try:
            df = pd.read_excel(TRADE_LOG_PATH)
            for col, dtype in TRADE_LOG_DTYPES.items():
                if col in df and df[col].dtype != dtype:
                    df[col] = df[col].astype(dtype, errors="ignore")
            return df
        except Exception as e:
            logger.error(f"Failed to read trade log: {e}")

    df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in TRADE_LOG_DTYPES.items()})
    df.to_excel(TRADE_LOG_PATH, index=False)
    return df"""

content = content.replace(old_load_trade_log, new_load_trade_log)

with open("ulpl_prepecity.py", "w") as f:
    f.write(content)
