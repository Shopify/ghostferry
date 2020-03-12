require "test_helper"
require "json"

class ForeignKeyTest < GhostferryTestCase
  def setup
    seed_simple_database_with_fk_constraints
    disable_foreign_key_constraints
  end

  def teardown
    enable_foreign_key_constraints
  end

  def test_copy_data_with_fk_constraints_writes_to_source
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: "true" })

    ghostferry.run
    assert_test_table_is_identical
  end 
end
