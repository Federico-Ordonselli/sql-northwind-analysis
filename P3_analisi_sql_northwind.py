"""
PROGETTO 3 – Analisi SQL su Database Northwind
===============================================
Questo script scarica automaticamente il database Northwind (SQLite),
esegue query analitiche avanzate e genera un report con grafici.

NON serve installare nulla oltre a pandas e matplotlib.

Esegui: python analisi_sql_northwind.py

Output: grafici PNG + northwind_report.csv
"""

import sqlite3
import urllib.request
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

sns.set_theme(style="whitegrid", palette="Blues_d")
plt.rcParams["figure.dpi"] = 120

# ── 1. SETUP DATABASE ────────────────────────────────────────────────────────
print("=" * 60)
print("ANALISI SQL – DATABASE NORTHWIND")
print("=" * 60)

DB_PATH = "northwind.db"

if not os.path.exists(DB_PATH):
    print("\n[1/6] Download database Northwind...")
    url = "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db"
    urllib.request.urlretrieve(url, DB_PATH)
    print(f"      ✅ Database scaricato: {DB_PATH}")
else:
    print(f"\n[1/6] Database già presente: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)

# Mostra tabelle disponibili
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
print(f"      Tabelle disponibili: {', '.join(tables['name'].tolist())}")

# ── 2. QUERY ANALITICHE ──────────────────────────────────────────────────────
print("\n[2/6] Esecuzione query SQL...")

# ── Query 1: Revenue per categoria prodotto
q1 = """
SELECT
    c.CategoryName                              AS categoria,
    COUNT(DISTINCT od.OrderID)                  AS n_ordini,
    SUM(od.Quantity)                            AS quantita_totale,
    ROUND(SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)), 2) AS revenue_netto
FROM [Order Details] od
JOIN Products p   ON od.ProductID   = p.ProductID
JOIN Categories c ON p.CategoryID   = c.CategoryID
GROUP BY c.CategoryName
ORDER BY revenue_netto DESC;
"""
df_cat = pd.read_sql(q1, conn)
print(f"\n  ✅ Query 1: Revenue per categoria ({len(df_cat)} categorie)")

# ── Query 2: Top 10 clienti per revenue
q2 = """
SELECT
    cu.CompanyName                                                      AS cliente,
    cu.Country                                                          AS paese,
    COUNT(DISTINCT o.OrderID)                                           AS n_ordini,
    ROUND(SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)), 2)      AS revenue_totale,
    ROUND(AVG(od.UnitPrice * od.Quantity * (1 - od.Discount)), 2)      AS avg_per_riga
FROM Customers cu
JOIN Orders o         ON cu.CustomerID = o.CustomerID
JOIN [Order Details] od ON o.OrderID   = od.OrderID
GROUP BY cu.CompanyName, cu.Country
ORDER BY revenue_totale DESC
LIMIT 10;
"""
df_top_clients = pd.read_sql(q2, conn)
print(f"  ✅ Query 2: Top 10 clienti ({len(df_top_clients)} righe)")

# ── Query 3: Revenue mensile (trend temporale)
q3 = """
SELECT
    SUBSTR(o.OrderDate, 1, 7)                                           AS mese,
    COUNT(DISTINCT o.OrderID)                                           AS n_ordini,
    ROUND(SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)), 2)      AS revenue
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
GROUP BY mese
ORDER BY mese;
"""
df_monthly = pd.read_sql(q3, conn)
print(f"  ✅ Query 3: Revenue mensile ({len(df_monthly)} mesi)")

# ── Query 4: Performance venditori (Employees)
q4 = """
SELECT
    e.FirstName || ' ' || e.LastName                                    AS venditore,
    e.Title                                                             AS ruolo,
    COUNT(DISTINCT o.OrderID)                                           AS n_ordini,
    ROUND(SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)), 2)      AS revenue_totale
FROM Employees e
JOIN Orders o           ON e.EmployeeID = o.EmployeeID
JOIN [Order Details] od ON o.OrderID   = od.OrderID
GROUP BY e.EmployeeID
ORDER BY revenue_totale DESC;
"""
df_employees = pd.read_sql(q4, conn)
print(f"  ✅ Query 4: Performance venditori ({len(df_employees)} dipendenti)")

# ── Query 5: Prodotti a rischio esaurimento (Window Function)
q5 = """
SELECT
    p.ProductName                   AS prodotto,
    c.CategoryName                  AS categoria,
    p.UnitsInStock                  AS scorta_attuale,
    p.ReorderLevel                  AS livello_riordino,
    p.UnitsOnOrder                  AS in_ordine,
    CASE
        WHEN p.UnitsInStock = 0         THEN 'ESAURITO'
        WHEN p.UnitsInStock <= p.ReorderLevel THEN 'CRITICO'
        ELSE 'OK'
    END                             AS stato_scorta
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
WHERE p.Discontinued = 0
ORDER BY p.UnitsInStock ASC
LIMIT 15;
"""
df_stock = pd.read_sql(q5, conn)
print(f"  ✅ Query 5: Stato scorte prodotti")

conn.close()

# ── 3. STAMPA RISULTATI ──────────────────────────────────────────────────────
print("\n[3/6] Insight principali...")

print(f"\n  📦 REVENUE PER CATEGORIA:")
for _, row in df_cat.iterrows():
    bar = "█" * int(row["revenue_netto"] / df_cat["revenue_netto"].max() * 20)
    print(f"  {row['categoria']:20} ${row['revenue_netto']:>10,.0f}  {bar}")

print(f"\n  👑 TOP 5 CLIENTI:")
for _, row in df_top_clients.head(5).iterrows():
    print(f"  {row['cliente'][:30]:32} ${row['revenue_totale']:>9,.0f}  [{row['paese']}]")

print(f"\n  🏆 PERFORMANCE VENDITORI:")
for _, row in df_employees.iterrows():
    print(f"  {row['venditore']:25} {row['n_ordini']:3} ordini  ${row['revenue_totale']:>9,.0f}")

critico = df_stock[df_stock["stato_scorta"].isin(["ESAURITO","CRITICO"])]
print(f"\n  ⚠️ PRODOTTI CRITICI O ESAURITI: {len(critico)}")
for _, row in critico.iterrows():
    print(f"  [{row['stato_scorta']:8}] {row['prodotto'][:40]:42} scorta: {row['scorta_attuale']}")

# ── 4. GRAFICI ───────────────────────────────────────────────────────────────
print("\n[4/6] Generazione grafici...")

fig, axes = plt.subplots(2, 2, figsize=(16, 11))
fig.suptitle("Analisi SQL – Database Northwind", fontsize=16, fontweight="bold")

# — Revenue per categoria
ax1 = axes[0, 0]
colors = sns.color_palette("Blues_d", len(df_cat))[::-1]
ax1.barh(df_cat["categoria"], df_cat["revenue_netto"], color=colors)
ax1.set_title("Revenue Netto per Categoria", fontweight="bold")
ax1.set_xlabel("Revenue ($)")
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}k"))
ax1.invert_yaxis()

# — Top 10 clienti
ax2 = axes[0, 1]
colors2 = sns.color_palette("Blues_d", len(df_top_clients))[::-1]
ax2.barh(df_top_clients["cliente"].str[:25], df_top_clients["revenue_totale"], color=colors2)
ax2.set_title("Top 10 Clienti per Revenue", fontweight="bold")
ax2.set_xlabel("Revenue ($)")
ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}k"))
ax2.invert_yaxis()

# — Trend mensile
ax3 = axes[1, 0]
x_indices = list(range(len(df_monthly)))
ax3.plot(x_indices, df_monthly["revenue"], marker="o", markersize=3, color="#1F4E79", linewidth=2)
ax3.fill_between(x_indices, df_monthly["revenue"], alpha=0.15, color="#1F4E79")
ax3.set_title("Trend Revenue Mensile", fontweight="bold")
ax3.set_xlabel("Anno"); ax3.set_ylabel("Revenue ($)")
# Mostra solo il primo mese di ogni anno come etichetta
anni = df_monthly["mese"].str[:4]  # es. "2012"
tick_positions = [i for i, a in enumerate(anni) if i == 0 or a != anni.iloc[i-1]]
ax3.set_xticks(tick_positions)
ax3.set_xticklabels(anni.iloc[tick_positions], rotation=0, ha="center", fontsize=9)
ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}k"))

# — Performance venditori
ax4 = axes[1, 1]
colors3 = sns.color_palette("Blues_d", len(df_employees))[::-1]
ax4.bar(df_employees["venditore"].str.split().str[0], df_employees["revenue_totale"], color=colors3)
ax4.set_title("Revenue per Venditore", fontweight="bold")
ax4.set_xlabel("Venditore"); ax4.set_ylabel("Revenue ($)")
ax4.tick_params(axis="x", rotation=30)
ax4.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}k"))

plt.tight_layout()
plt.savefig("analisi_northwind.png", bbox_inches="tight")
plt.show()
print("      ✅ Grafico salvato: analisi_northwind.png")

# ── 5. EXPORT ────────────────────────────────────────────────────────────────
print("\n[5/6] Export CSV...")
df_cat.to_csv("northwind_categorie.csv", index=False)
df_top_clients.to_csv("northwind_top_clienti.csv", index=False)
df_stock.to_csv("northwind_scorte.csv", index=False)
print("      ✅ northwind_categorie.csv")
print("      ✅ northwind_top_clienti.csv")
print("      ✅ northwind_scorte.csv")

print("\n[6/6] Completato.")
print("\n" + "=" * 60)
print("ANALISI SQL NORTHWIND COMPLETATA")
print("=" * 60)
