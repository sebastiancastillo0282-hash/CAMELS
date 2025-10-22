# CAMELS MVP Delivery Plan

This document captures the phased implementation roadmap for the CAMELS risk dashboard MVP as described in the PRD.

## Phase 0 – Project Scaffolding & Single Command Workflow
- Establish the `camels/` package structure with submodules for ingestion, normalization, scoring, dashboard, export, and audit.
- Provide project tooling (requirements, pyproject, optional container) and expose a single CLI entry point (`camels run`) orchestrating end-to-end execution.
- Supply environment templates (`.env.example`), logging configuration, and keep the README ≤500 palabras covering setup and execution.

## Phase 1 – Official Data Ingestion Automation
- Catalog regulator sources in `config/sources.yaml` with URLs, formatos (CSV/XLSX/PDF), frecuencia, país/regulador y bancos objetivo.
- Implement download utilities with reintentos, verificación de hash y logging; persist raw files bajo `data/raw/<fecha>/`.
- Build format-specific parsers (CSV/XLSX/PDF) to extract indicadores relevantes y registrar cada descarga en SQLite (`ingestion_log`).

## Phase 2 – Indicator Normalization & Historical Persistence
- Diseñar el esquema SQLite (`banks`, `indicators`, `indicator_history`) garantizando relación país/regulador.
- Implementar transformaciones que conviertan unidades, agreguen por trimestre y retengan al menos 8–12 trimestres históricos.
- Población inicial de la tabla `banks` con >50 bancos seed y validaciones que detecten gaps, duplicados o valores atípicos antes de persistir.

### Lista Seed de Bancos
- Citibank N.A. (EE.UU) — Estados Unidos
- Bank of America N.A. (EE.UU) — Estados Unidos
- Bradesco BAC Florida (EE.UU) — Estados Unidos
- HSBC (USA) — Estados Unidos
- Scotiabank (Canadá) — Canada
- Banco G&T Continental, S.A. — Guatemala
- Banco Industrial, S.A. — Guatemala
- Banco Promerica, S.A. — Guatemala
- Banco Ficohsa Guatemala — Guatemala
- Banco de América Central, S.A. (BAC Credomatic GT) — Guatemala
- Citibank N.A. Sucursal Guatemala — Guatemala
- Banco de Desarrollo Rural, S.A. (Banrural) — Guatemala
- Crédito Hipotecario Nacional (CHN) — Guatemala
- Banco Internacional, S.A. (InterBanco) — Guatemala
- Banco Inmobiliario, S.A. — Guatemala
- Banco LAFISE, S.A. — Guatemala
- Banco Agrícola, S.A. — El Salvador
- Banco Promerica, S.A. — El Salvador
- Banco Industrial El Salvador, S.A. — El Salvador
- Citibank N.A. Sucursal El Salvador — El Salvador
- Banco Davivienda Salvadoreño — El Salvador
- Banco de América Central, S.A. (BAC El Salvador) — El Salvador
- Banco LAFISE El Salvador — El Salvador
- Banco del País, S.A. (Banpaís) — Honduras
- Banco Ficohsa Honduras — Honduras
- Banco de Occidente, S.A. — Honduras
- Citibank N.A. Sucursal Honduras — Honduras
- Banco Atlántida, S.A. — Honduras
- Banco de América Central Honduras (BAC Honduras) — Honduras
- Banco Davivienda Honduras — Honduras
- Banco LAFISE Bancentro — Nicaragua
- Banco de la Producción, S.A. (Banpro) — Nicaragua
- Banco de América Central Nicaragua (BAC) — Nicaragua
- Banco Nacional de Costa Rica (BNCR) — Costa Rica
- Citibank N.A. Sucursal Costa Rica — Costa Rica
- Scotiabank Costa Rica — Costa Rica
- Banco Davivienda Costa Rica — Costa Rica
- Bi Bank, S.A. (Grupo BI) — Panamá
- BAC International Bank, Inc. (BAC Panamá) — Panamá
- Banco General, S.A. — Panamá
- Banco Ficohsa (Panamá), S.A. — Panamá
- Banco LAFISE Panamá, S.A. — Panamá
- Citibank N.A. Sucursal Panamá — Panamá
- Scotiabank (Panamá) — Panamá
- Banco Popular Dominicano — República Dominicana
- Citibank N.A. Sucursal R.D. — República Dominicana
- Banco Ve por Más, S.A. (Bx+) — México
- Citibanamex (Banco Nacional de México) — México

## Phase 3 – Configurable CAMELS Scoring Engine
- Definir umbrales y pesos en `config/camels_thresholds.yaml` para cada pilar y score compuesto.
- Construir `camels/scoring/engine.py` para evaluar indicadores, asignar puntajes por pilar y semáforos, y guardar resultados en `scores` y `pillar_scores`.
- Exponer comando `camels score` integrable con la ejecución completa y registrar cada corrida con timestamp.

## Phase 4 – Dashboard de Visualización (Streamlit)
- Crear `dashboard/app.py` con vistas Portfolio, Banco, País/Regulador y Auditoría.
- Implementar componentes reutilizables (tabla semáforo, radar CAMELS, gráficas históricas) enlazados a la base SQLite mediante `dashboard/data_access.py`.
- Añadir filtros dinámicos (país, regulador, rango temporal), exportaciones CSV/Excel y enlaces de trazabilidad hacia los documentos fuente.

## Phase 5 – Auditoría, Trazabilidad y Exports Consolidados
- Registrar metadatos de origen (URL, fecha descarga, hash, versión pipeline) en `audit_trail` y permitir descarga desde CLI/UI.
- Construir panel de auditoría en el dashboard y generadores (`camels/export/generators.py`) para consolidar scores e indicadores en CSV/Excel.
- Asegurar trazabilidad end-to-end: cada métrica en el dashboard enlaza al documento oficial correspondiente.

## Phase 6 – QA, Testing & Release Readiness
- Cubrir ingestion, normalization, scoring y dashboard con pruebas unitarias/integración (pytest) y linters (ruff/black).
- Configurar CI/automatización local para ejecutar pruebas, validaciones y generación de artefactos demo.
- Preparar scripts de carga de datos ejemplo y documentar troubleshooting, operación offline y actualización de thresholds.

## Acceptance Checkpoints
- Automatización completa: una ejecución de `camels run` descarga, normaliza, puntúa y habilita el dashboard.
- Dashboard con ranking, radar, tendencias históricas (8–12 trimestres) y vista país/regulador.
- Auditoría disponible mostrando fuente, URL, fecha y hash, con exportación consolidada lista para comité.
