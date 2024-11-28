package main

import "testing"

func TestShouldSyncTable(t *testing.T) {
	t.Run("returns true when no filters are set", func(t *testing.T) {
		config := &Config{
			Pg: PgConfig{
				DatabaseUrl: "postgres://user:pass@localhost:5432/db",
			},
		}
		syncer := NewSyncer(config)
		table := SchemaTable{Schema: "public", Table: "users"}

		if !syncer.shouldSyncTable(table) {
			t.Error("Expected shouldSyncTable to return true when no filters are set")
		}
	})

	t.Run("respects include filter", func(t *testing.T) {
		config := &Config{
			Pg: PgConfig{
				DatabaseUrl:   "postgres://user:pass@localhost:5432/db",
				IncludeTables: NewSet([]string{"public.users", "public.orders"}),
			},
		}
		syncer := NewSyncer(config)

		included := SchemaTable{Schema: "public", Table: "users"}
		if !syncer.shouldSyncTable(included) {
			t.Error("Expected shouldSyncTable to return true for included table")
		}

		excluded := SchemaTable{Schema: "public", Table: "secrets"}
		if syncer.shouldSyncTable(excluded) {
			t.Error("Expected shouldSyncTable to return false for non-included table")
		}
	})

	t.Run("respects exclude filter", func(t *testing.T) {
		config := &Config{
			Pg: PgConfig{
				DatabaseUrl:   "postgres://user:pass@localhost:5432/db",
				ExcludeTables: NewSet([]string{"public.secrets", "public.cache"}),
			},
		}
		syncer := NewSyncer(config)

		included := SchemaTable{Schema: "public", Table: "users"}
		if !syncer.shouldSyncTable(included) {
			t.Error("Expected shouldSyncTable to return true for non-excluded table")
		}

		excluded := SchemaTable{Schema: "public", Table: "secrets"}
		if syncer.shouldSyncTable(excluded) {
			t.Error("Expected shouldSyncTable to return false for excluded table")
		}
	})
}
