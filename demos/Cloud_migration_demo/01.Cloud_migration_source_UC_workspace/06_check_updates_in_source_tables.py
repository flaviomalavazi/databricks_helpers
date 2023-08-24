# Databricks notebook source
# MAGIC %sql
# MAGIC select max(dinner_id) as latest_dinner from SOURCE_CLOUD.cloud_migration_demo.dinners
