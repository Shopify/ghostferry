require "test_helper"
require "json"

class ForeignKeyTest < GhostferryTestCase
  def setup
    seed_simple_database_with_fk_constraints
    disable_foreign_key_constraints
    disable_writes_on_source
  end

  def teardown
    enable_foreign_key_constraints
    enable_writes_on_source
  end

  def test_foreign_key_copy_data_without_writes_to_source
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true })
    ghostferry.run

    assert_test_table_is_identical(tables: DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS)
  end

  def test_foreign_key_interrupt_resume_idempotence_without_writes_to_source
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true })
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true })
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical(tables: DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS)

    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical(tables: DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS)
    assert_ghostferry_completed(ghostferry, times: 2)
  end

  # Consider a scenario where rows in parent table on source db are updated during interrupt, and due to CASCADES 
  # the rows on child table are updated as well, but due to foreign key checks are disabled, the rows in child table
  # on target db are not updated. This will lead to non-identical child tables in source and target db as a result
  # we should avoid updating rows on source db during interrupt.
  def test_foreign_key_verification_fails_with_rows_changed_on_source_during_interrupt
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true })

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written > 7
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    min_ids = []
    i = 0
    while i < DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS.length
      i += 1
      result = target_db.query("SELECT MIN(id#{i}) FROM #{DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS[i-1]}")
      min_ids << result.first["MIN(id#{i})"]
    end

    # Due to ON UPDATE CASCADE on test_fk_table2 when we change the id of a row in test_fk_table1 the column
    # id2 on test_fk_table2 will be automatically changed in source db, but not being written to the binlogs
    # As a result the even after resuming ghostferry from the dumped state the column with id2 = min_id + 1000000
    # won't be present on test_fk_table2 in target db.
    enable_writes_on_source
    choosen_id = min_ids.min
    random_value = 1000000
    source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS.first} SET id1 = #{choosen_id + random_value} WHERE id1 = #{choosen_id}")
    disable_writes_on_source

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true, verifier_type: "Inline" })
    ghostferry.run(dumped_state)

    source_result = source_db.query("CHECKSUM TABLE #{DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS[1]}")
    target_result = target_db.query("CHECKSUM TABLE #{DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS[1]}")
    refute_equal source_result.first["Checksum"], target_result.first["Checksum"]
  end

  # Consider a scenario where rows in parent table on source db are changed during interrupt, and due to CASCADES 
  # the rows on child table are updated as well, but due to foreign key checks are disabled, the rows in child table
  # on target db are not updated. Inline Verifier will still pass in this scenario even though the child table is clearly
  # not identical on source and target db. The reason is the rows in child table on source db were verified inline before
  # the interrupt happened and during the interrupt when these rows were changed, they were not emitted to the binlogs,
  # so these changed rows during interrupt never got passed to reverifyStore, and eventually Inline verifier passed.
  # This test is to depict the same scenario.
  def test_foreign_key_inline_verifier_passes_even_though_source_and_target_tables_are_not_identical
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true, verifier_type: "Inline" })

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written > 7
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    min_ids = []
    i = 0
    while i < DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS.length
      i += 1
      result = target_db.query("SELECT MIN(id#{i}) FROM #{DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS[i-1]}")
      min_ids << result.first["MIN(id#{i})"]
    end

    enable_writes_on_source
    choosen_id = min_ids.min
    random_value = 1000000
    source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAMES_WITH_FK_CONSTRAINTS.first} SET id1 = #{choosen_id + random_value} WHERE id1 = #{choosen_id}")
    disable_writes_on_source

    verification_ran = false
    incorrect_tables = []
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { skip_foreign_key_constraints_check: true, verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)
    assert verification_ran
    assert_equal incorrect_tables, []
  end
end
