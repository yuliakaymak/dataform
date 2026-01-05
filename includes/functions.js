// -------------------------------------------
// REUSABLE FUNCTIONS
// -------------------------------------------

/**
 * Function addSCDColumns
 * Generates SQL for SCD Type 2 valid from/to and is_active columns.
 *
 * @param {string} refTable - The source table name.
 * @param {string} timestampColumn - The timestamp column for ordering.
 * @param {string[]} partitionColumns - Columns to partition by in window function.
 * @returns {string} - SQL query string for the SCD logic.
 */
function addSCDColumns(refTable, timestampColumn, partitionColumns) {
    const {
        END_OF_VALIDITY_DATE
    } = require('./vars');
    const partitionBy = partitionColumns.join(", ");
    const partitionByExpr = `partition by ${partitionBy}`;
    const orderByExpr = `order by ${timestampColumn}`;
    const leadExpr = `lead(${timestampColumn}) over (${partitionByExpr} ${orderByExpr})`;

    return `
        select
            *,
            ${timestampColumn} as _row_valid_from,
            coalesce(${leadExpr}, ${END_OF_VALIDITY_DATE}) as _row_valid_to,
            case when ${leadExpr} is null then 1 else 0 end as _is_active
        from ${refTable}  
  `;
}

/**
 * Function generateCaseColumn
 * Generates a CASE WHEN SQL expression.
 * 
 * @param {string} columnName - Name of the new SQL column.
 * @param {Object} conditions - Object where keys are WHEN conditions and values are THEN results.
 * @param {string | null} elseValue - Optional ELSE value.
 * @returns {string} SQL snippet for use inside a SELECT clause.
 */
function generateCaseColumn(columnName, conditions, elseValue = null) {
  const whenClauses = Object.entries(conditions)
    .map(([whenCondition, thenValue]) => `WHEN ${whenCondition} THEN ${thenValue}`)
    .join("\n        ");

  const elseClause = elseValue ? `ELSE ${elseValue}` : "";

  return `
    CASE
        ${whenClauses}
        ${elseClause}
    END AS ${columnName}
  `;
}

/**
 * Generates a CASE WHEN column and checks is value is in the list.
 *
 * @param {string} existingColumnName  - The column to check
 * @param {Array<string|number>} list  - The list to match against
 * @param {string} newColumnName       - The name of the output column
 *
 * @returns {string} SQL "CASE WHEN ... END AS ..."
 */
function caseWhenInList(existingColumnName, list, newColumnName) {
    return `
    CASE 
        WHEN ${existingColumnName} IN (${list.map(v => `'${v}'`).join(", ")}) 
        THEN 1 
        ELSE 0 END AS ${ newColumnName}
        `;
}

/**
 * Function runAssertRelationship
 * Generates SQL to assert that every value in a child column exists in a parent column.
 *
 * @param {string} childTable - The name of the child table.
 * @param {string} childColumn - The column in the child table.
 * @param {string} parentTable - The name of the parent table.
 * @param {string} parentColumn - The column in the parent table.
 * @returns {string} SQL assertion query.
 */

function runAssertRelationship(childTable, childColumn, parentTable, parentColumn) {
  return `
    select
    ${childColumn} as invalid_key
    from ${childTable}
    where ${childColumn} not in (
      select ${parentColumn} from ${parentTable}
    )
  `;
}

// ------------------------------------------------------ 
module.exports = {
    addSCDColumns,
    generateCaseColumn,
    runAssertRelationship,
    caseWhenInList,
};
