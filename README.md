# 🚲 Análisis Distribuido de Movilidad CDMX con PySpark

Este proyecto realiza un análisis distribuido de datos reales de movilidad urbana en la Ciudad de México usando Apache Spark y Docker. Se procesan millones de registros del sistema ECOBICI para identificar patrones de uso y zonas con mayor congestión.

---

## 📦 Requisitos

* Docker
* Docker Compose

---

## ⚙️ Instalación y ejecución

1. Clona este repositorio:

   ```bash
   git clone https://github.com/usuario/proyecto-pyspark-ecobici.git
   cd ecobici-spark
   ```

2. Descarga los archivos CSV desde:

   👉 [https://ecobici.cdmx.gob.mx/datos-abiertos](https://ecobici.cdmx.gob.mx/datos-abiertos)

   y colócalos en la carpeta `data/` (debe crearse si no existe).

3. Construye y ejecuta el contenedor de Spark:

   ```bash
   docker compose up --build
   ```

   Esto ejecutará el análisis y generará los resultados en la carpeta `results/`.

---

## 📁 Estructura del proyecto

```
.
├── data/                    # Archivos CSV originales (no versionados)
├── results/                 # Archivos de salida (.csv y .png)
├── src/
│   ├── analysis.py          # Script principal (driver)
│   ├── cleaning.py          # Pipeline de limpieza y normalización
│   └── schema.py            # Esquema unificado para Spark
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 📊 Objetivo

* Identificar rutas más utilizadas (congestión)
* Detectar horarios con mayor actividad
* Analizar balance de estaciones por llegadas/salidas
* Calcular duración promedio de viajes

---

## 🧪 Tecnología usada

* Apache Spark (PySpark)
* Docker (imagen oficial de Bitnami Spark)
* Matplotlib para visualización de resultados

---

## 🗂️ Archivos generados

* `results/resumen_ecobici.csv` → resumen de métricas clave
* `results/top_rutas.csv` → top 20 rutas más transitadas
* `results/heatmap_horarios.png` → gráfico de calor de viajes por hora/día
* `results/top_rutas.png` → gráfico de barras de rutas más concurridas
