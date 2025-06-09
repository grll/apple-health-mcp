#!/usr/bin/env python3
"""Check the types of records in the database."""

from sqlmodel import Session, create_engine, select, func
from src.apple_health_mcp.models import Record

# Create engine
engine = create_engine("sqlite:///data/sqlite.db")

with Session(engine) as session:
    # Get record type counts
    stmt = select(Record.type, func.count(Record.id)).group_by(Record.type).order_by(func.count(Record.id).desc())
    results = session.exec(stmt).all()
    
    print("Record types and counts:")
    print("-" * 50)
    for record_type, count in results[:20]:  # Top 20 types
        print(f"{record_type}: {count:,}")