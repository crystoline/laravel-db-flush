<?php

namespace Crystoline\LaravelDbFlush;

use Illuminate\Support\Facades\DB;
use Prophecy\Exception\Doubler\MethodNotFoundException;

class Sql{

    private $connection;
    private $driver;

    private static $attributes = [
        'concat'=>[
            'mysql'=> 'concat',
            'sqlsrv'=> 'concat',
        ],
        'if'=>[
            'mysql'=> 'if',
            'sqlsrv'=> 'iif',
        ],
        'yyyy'=>[
            'mysql'=> '%Y',
            'sqlsrv'=> 'yyyy',
        ],
        'mm'=>[
            'mysql'=> '%m',
            'sqlsrv'=> 'MM',
        ],
        'dd'=>[
            'mysql'=> '%d',
            'sqlsrv'=> 'dd',
        ],
        'is_null'=>[
            'mysql'=> 'is null',
            'sqlsrv'=> '= null',
        ],
        'disable_checks'=>[
            'mysql'=> 'set foreign_key_checks = 0;',
            'sqlsrv'=> 'EXEC sp_MSforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT all"',
        ],
        'enable_checks'=>[
            'mysql'=> 'set foreign_key_checks = 1;',
            'sqlsrv'=> 'EXEC sp_MSforeachtable "ALTER TABLE ? CHECK CONSTRAINT all"',
        ],
        'kill_checks'=>[
            'mysql'=>'set foreign_key_checks = 0;',
            'sqlsrv'=>"
             Set NoCount ON

                Declare @schemaName varchar(200)
                set @schemaName=''
                Declare @constraintName varchar(200)
                set @constraintName=''
                Declare @tableName varchar(200)
                set @tableName=''

                While exists
                (
                    SELECT c.name
                    FROM sys.objects AS c
                    INNER JOIN sys.tables AS t
                    ON c.parent_object_id = t.[object_id]
                    INNER JOIN sys.schemas AS s
                    ON t.[schema_id] = s.[schema_id]
                    WHERE c.[type] IN ('D','C','F','UQ')
                    and t.[name] NOT IN ('__RefactorLog', 'sysdiagrams')
                    and c.name > @constraintName
                )

                Begin
                    -- First get the Constraint
                    SELECT
                        @constraintName=min(c.name)
                    FROM sys.objects AS c
                    INNER JOIN sys.tables AS t
                    ON c.parent_object_id = t.[object_id]
                    INNER JOIN sys.schemas AS s
                    ON t.[schema_id] = s.[schema_id]
                    WHERE c.[type] IN ('D','C','F','UQ')
                    and t.[name] NOT IN ('__RefactorLog', 'sysdiagrams')
                    and c.name > @constraintName

                    -- Then select the Table and Schema associated to the current constraint
                    SELECT
                        @tableName = t.name,
                        @schemaName = s.name
                    FROM sys.objects AS c
                    INNER JOIN sys.tables AS t
                    ON c.parent_object_id = t.[object_id]
                    INNER JOIN sys.schemas AS s
                    ON t.[schema_id] = s.[schema_id]
                    WHERE c.name = @constraintName

                    -- Then Print to the output and drop the constraint
                    Print 'Dropping constraint ' + @constraintName + '...'
                    Exec('ALTER TABLE [' + @tableName + N'] DROP CONSTRAINT [' + @constraintName + ']')
                End

                Set NoCount OFF"
        ]
    ];

    function __construct()
    {
        $this->connection = env('DB_CONNECTION');
        $this->driver = config('database.connections.'.$this->connection.'.driver');
    }

    function instance()
    {
        return $this;
    }

    function connection($connection)
    {
        $this->connection = $connection;
        $this->driver = config('database.connections.'.$connection.'.driver');
        return $this;
    }

    function db()
    {
        return DB::connection($this->connection);
    }

    /**
     * @return mixed
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @return mixed
     */
    public function getDriver()
    {
        return $this->driver;
    }

    function __call($method,$args)
    {
        if(method_exists($this,"get".studly_case($method))){
            $m = "get".studly_case($method);
            $result = $this->$m($args);
        }

        if(isset($result)) return isset($result[$this->driver])?$result[$this->driver]:$result['mysql'];

        throw new MethodNotFoundException("Call to undefined method ".static::class."::".$method,static::class,$method);
    }

    function __get($attribute)
    {
        if(isset(static::$attributes[snake_case($attribute)])){
            $result = static::$attributes[snake_case($attribute)];
        }

        if(isset($result) and isset($result[$this->driver])) return $result[$this->driver];

        return null;
    }

    function getDateFormat($args){
        return [
            'mysql' => "DATE_FORMAT($args[0], '$args[1]')",
            'sqlsrv' => "FORMAT($args[0], '$args[1]')"
        ];
    }

    function getDateAdd($args){
        return [
            'mysql' => "DATE_ADD($args[0],INTERVAL $args[1] $args[2])",
            'sqlsrv' => "DATEADD(".strtolower($args[2]).",$args[1],$args[0])"
        ];
    }

    function getNullsLast($args){
        return [
            'mysql' => "$args[0] IS NULL",
            'sqlsrv' => "CASE WHEN $args[0] IS NULL THEN 1 ELSE 0 END"
        ];
    }

    public function specificOrder($column,$array=[])
    {
        if(empty($array)) return $column;

        $sql = "case $column ";
        foreach ($array as $k=>$v){
            $sql .= "when '$v' then $k ";
        }
        $k++;
        $sql .= "else $k end";
        return $sql;
    }

    function showTables(){

        $schema = config('database.connections.'.$this->connection.'.database');

        switch($this->driver){
            case "sqlsrv":
                return array_map(function($obj){
                    return $obj->TABLE_NAME;
                },$this->db()->select("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = '$schema'"));
                break;
            default:
                return array_map(function($obj){
                    return $obj->TABLE_NAME;
                },$this->db()->select("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '$schema'"));
                break;
        }
    }

    function tableComment($query){
        switch($this->driver){
            case "sqlsrv":
                return null;
            default:
                return $this->db()->statement($query);
        }
    }

    function alterEnumColumn($table, $column, array $enums,$default = null,$comment = "", $after = ""){

        if(!$default) $default = reset($enums);

        switch($this->driver){
            case "sqlsrv":
                return true;
            default:
                return $this->db()->statement(
                    "ALTER TABLE `$table`
	                CHANGE COLUMN `$column` `$column` ENUM('".implode("','",$enums)."') 
	                NOT NULL DEFAULT '$default' COMMENT '$comment' COLLATE 'utf8_unicode_ci'
                    ".(!empty($after)?" AFTER `$after`;":";"));
        }
    }

    function alterTable($table,$column,$column_properties){
        switch($this->driver){
            case "sqlsrv":
                return $this->db()->statement("ALTER TABLE [$table] ALTER COLUMN [$column] $column_properties");
            default:
                return $this->db()->statement("ALTER TABLE `$table` MODIFY `$column` $column_properties");
        }
    }

    public function resetAutoIncrement($table)
    {
        switch($this->driver){
            case "sqlsrv":
                return null;
            default:
                return $this->db()->statement("ALTER TABLE $table AUTO_INCREMENT = 1");
        }
    }

    public function groupByYearMonth($column='created_at')
    {
        switch($this->driver){
            case "sqlsrv":
                return "convert(varchar(7),$column,21)";
            default:
                return "EXTRACT(YEAR_MONTH FROM $column)";
        }
    }

    public function groupByYear($column='created_at')
    {
        switch($this->driver){
            case "sqlsrv":
                return "convert(varchar(4),$column,21)";
            default:
                return "EXTRACT(YEAR FROM $column)";
        }
    }

    public function escaped($string)
    {
        switch($this->driver){
            case "sqlsrv":
                return str_replace("`",'"',str_replace('"',"'",$string));
            default:
                return $string;
        }
    }

    public function morph($string)
    {
        switch($this->driver){
            case "sqlsrv":
                return $string;
            default:
                return str_replace('\\','\\\\',$string);
        }
    }

    public function getDefaultConstraint($table,$column){
        switch($this->driver){
            case "sqlsrv":
                $filter = "DF__".substr($table,0,9)."__".substr($column,0,5)."%";
                $result = $this->db()->select("SELECT * FROM sysobjects WHERE xtype = 'D' and name like '$filter'");
                if(!empty($result)){
                    return $result[0]->name;
                }
                return null;
            default:
                return null;
        }

    }

    public function dropDefaultConstraint($table,$column){
        if($constraint = $this->getDefaultConstraint($table, $column)){
            return $this->db()->statement("ALTER TABLE $table DROP CONSTRAINT $constraint;");
        }
        return false;
    }

}