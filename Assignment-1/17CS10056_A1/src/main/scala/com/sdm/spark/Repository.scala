package com.sdm.spark

import java.util.Date

case class Repository(id: Integer, url: String, ownerId: Integer,
                name: String, language: String, createdAt: Date, forkedFrom: String, deleted: Integer, updatedAt: Date)
