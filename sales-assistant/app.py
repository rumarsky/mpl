import streamlit as st
import pandas as pd

from db import run_query
from sql_validator import is_safe_sql
from llm_module import generate_sql_from_nl, generate_text_explanation

import sqlite3


st.set_page_config(page_title="Sales Analytics Assistant", layout="wide")

st.title("🧠 Умный аналитический ассистент по продажам (локальная LLM)")


# Инициализация состояния сессии
if "messages" not in st.session_state:
    st.session_state["messages"] = []  # список словарей: {role, content, df?, chart?}


# --- SIDEBAR: загрузка динамических таблиц ---
st.sidebar.header("📂 Динамические таблицы")

uploaded_file = st.sidebar.file_uploader("Загрузить CSV как новую таблицу", type=["csv"])
new_table_name = st.sidebar.text_input("Имя новой таблицы в БД (латиница)", "")

if uploaded_file is not None and new_table_name:
    try:
        df_new = pd.read_csv(uploaded_file)
        conn = sqlite3.connect("sales.db")
        df_new.to_sql(new_table_name, conn, if_exists="replace", index=False)
        conn.close()
        st.sidebar.success(f"Таблица '{new_table_name}' добавлена в БД.")
    except Exception as e:
        st.sidebar.error(f"Ошибка при добавлении таблицы: {e}")


# --- Вывод истории сообщений ---
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "df" in msg and msg["df"] is not None:
            st.dataframe(msg["df"])
        if msg.get("chart") == "bar" and "df" in msg and msg["df"] is not None:
            try:
                st.bar_chart(msg["df"].set_index(msg["df"].columns[0]))
            except Exception:
                pass


# --- Ввод пользователя ---
user_input = st.chat_input("Задай вопрос о продажах (например: 'Покажи топ-5 товаров по выручке за 2024 год')")

if user_input:
    # добавляем сообщение пользователя в историю
    st.session_state["messages"].append({
        "role": "user",
        "content": user_input,
    })

    # сообщение ассистента (ответ)
    with st.chat_message("assistant"):
        with st.spinner("Генерирую SQL через локальную модель..."):
            try:
                sql = generate_sql_from_nl(user_input)
            except Exception as e:
                st.error(f"Ошибка при обращении к локальной LLM: {e}")
                # добавим короткий ответ в историю и выйдем
                st.session_state["messages"].append({
                    "role": "assistant",
                    "content": f"Произошла ошибка при обращении к локальной модели: {e}",
                })
                st.stop()

        st.subheader("Сгенерированный SQL-запрос")
        st.code(sql, language="sql")

        if not is_safe_sql(sql):
            error_text = (
                "Сгенерированный SQL-запрос был признан небезопасным и не выполнен. "
                "Попробуй переформулировать запрос."
            )
            st.error(error_text)
            st.session_state["messages"].append({
                "role": "assistant",
                "content": error_text,
            })
        else:
            # Выполнение SQL
            try:
                df = run_query(sql)
            except Exception as e:
                err_msg = f"Ошибка при выполнении SQL-запроса: {e}"
                st.error(err_msg)
                st.session_state["messages"].append({
                    "role": "assistant",
                    "content": err_msg,
                })
            else:
                # Объяснение результата
                if df.empty:
                    explanation = "Запрос выполнен, но по указанным условиям нет данных (пустой результат)."
                else:
                    with st.spinner("Анализирую результат запроса..."):
                        try:
                            explanation = generate_text_explanation(user_input, sql, df)
                        except Exception:
                            # если модель подвисла, fallback
                            explanation = "Запрос выполнен. Ниже показана выборка данных и график."

                st.markdown(explanation)
                st.dataframe(df)

                chart_type = None
                if not df.empty and df.shape[1] >= 2:
                    try:
                        st.bar_chart(df.set_index(df.columns[0]))
                        chart_type = "bar"
                    except Exception:
                        chart_type = None

                # сохраняем ответ ассистента в историю
                st.session_state["messages"].append({
                    "role": "assistant",
                    "content": explanation,
                    "df": df,
                    "chart": chart_type,
                })
