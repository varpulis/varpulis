// Tree-sitter grammar for VPL
// A CEP (Complex Event Processing) language

module.exports = grammar({
    name: 'varpulis',

    extras: $ => [
        /\s/,
        $.comment,
    ],

    word: $ => $.identifier,

    conflicts: $ => [
        [$.primary_expression, $.lambda_expression],
        [$.call_expression, $.lambda_expression],
    ],

    precedences: $ => [
        [
            'unary',
            'power',
            'multiplicative',
            'additive',
            'shift',
            'bitwise_and',
            'bitwise_xor',
            'bitwise_or',
            'comparison',
            'not',
            'and',
            'or',
            'lambda',
        ],
    ],

    rules: {
        // Entry point
        source_file: $ => repeat($._statement),

        _statement: $ => choice(
            $.stream_declaration,
            $.event_declaration,
            $.type_declaration,
            $.variable_declaration,
            $.constant_declaration,
            $.function_declaration,
            $.config_block,
            $.import_statement,
            $.if_statement,
            $.for_statement,
            $.while_statement,
            $.return_statement,
            $.break_statement,
            $.continue_statement,
            $.expression_statement,
        ),

        // ========================================================================
        // Stream Declaration
        // ========================================================================

        stream_declaration: $ => seq(
            'stream',
            field('name', $.identifier),
            optional(seq(':', field('type', $._type))),
            choice(
                seq('from', field('source', $.identifier)),
                seq('=', field('expression', $.stream_expression)),
            ),
        ),

        stream_expression: $ => seq(
            field('source', $.stream_source),
            repeat($.stream_operation),
        ),

        stream_source: $ => choice(
            $.merge_source,
            $.join_source,
            $.sequence_source,
            $.all_source,
            $.aliased_source,
            $.identifier,
        ),

        merge_source: $ => seq(
            'merge',
            '(',
            commaSep1($.inline_stream),
            ')',
        ),

        join_source: $ => seq(
            'join',
            '(',
            commaSep1($.join_clause),
            ')',
        ),

        sequence_source: $ => seq(
            'sequence',
            '(',
            commaSep1($.sequence_step),
            ')',
        ),

        all_source: $ => seq(
            'all',
            field('name', $.identifier),
            optional(seq('as', field('alias', $.identifier))),
        ),

        aliased_source: $ => seq(
            field('name', $.identifier),
            'as',
            field('alias', $.identifier),
        ),

        inline_stream: $ => choice(
            seq(
                'stream',
                field('name', $.identifier),
                'from',
                field('source', $.identifier),
                optional(seq('where', field('filter', $._expression))),
            ),
            $.identifier,
        ),

        join_clause: $ => choice(
            seq(
                'stream',
                field('name', $.identifier),
                'from',
                field('source', $.identifier),
                optional(seq('on', field('condition', $._expression))),
            ),
            $.identifier,
        ),

        sequence_step: $ => seq(
            field('alias', $.identifier),
            ':',
            field('event_type', $.identifier),
            optional(seq('where', field('filter', $._expression))),
            optional(seq('.', 'within', '(', field('timeout', $._expression), ')')),
        ),

        // Stream operations
        stream_operation: $ => choice(
            $.dot_operation,
            $.followed_by_operation,
        ),

        dot_operation: $ => seq(
            '.',
            choice(
                $.where_op,
                $.select_op,
                $.window_op,
                $.aggregate_op,
                $.partition_by_op,
                $.order_by_op,
                $.limit_op,
                $.distinct_op,
                $.map_op,
                $.filter_op,
                $.tap_op,
                $.print_op,
                $.log_op,
                $.emit_op,
                $.to_op,
                $.pattern_op,
                $.concurrent_op,
                $.process_op,
                $.on_error_op,
                $.collect_op,
                $.on_op,
                $.within_op,
                $.not_op,
                $.fork_op,
                $.any_op,
                $.all_op,
                $.first_op,
                $.forecast_op,
                $.enrich_op,
            ),
        ),

        where_op: $ => seq('where', '(', $._expression, ')'),
        select_op: $ => seq('select', '(', commaSep1($.select_item), ')'),
        window_op: $ => seq('window', '(', $.window_args, ')'),
        aggregate_op: $ => seq('aggregate', '(', commaSep1($.agg_item), ')'),
        partition_by_op: $ => seq('partition_by', '(', $._expression, ')'),
        order_by_op: $ => seq('order_by', '(', commaSep1($.order_item), ')'),
        limit_op: $ => seq('limit', '(', $._expression, ')'),
        distinct_op: $ => seq('distinct', '(', optional($._expression), ')'),
        map_op: $ => seq('map', '(', $._expression, ')'),
        filter_op: $ => seq('filter', '(', $._expression, ')'),
        tap_op: $ => seq('tap', '(', commaSep1($.named_argument), ')'),
        print_op: $ => seq('print', '(', optional(commaSep1($._expression)), ')'),
        log_op: $ => seq('log', '(', optional(commaSep1($.named_argument)), ')'),
        emit_op: $ => seq('emit', '(', optional(commaSep1($.named_argument)), ')'),
        to_op: $ => seq('to', '(', $._expression, ')'),
        pattern_op: $ => seq('pattern', '(', $.pattern_def, ')'),
        concurrent_op: $ => seq('concurrent', '(', commaSep1($.named_argument), ')'),
        process_op: $ => seq('process', '(', $._expression, ')'),
        on_error_op: $ => seq('on_error', '(', $._expression, ')'),
        collect_op: $ => seq('collect', '(', ')'),
        on_op: $ => seq('on', '(', $._expression, ')'),
        within_op: $ => seq('within', '(', $._expression, ')'),
        not_op: $ => seq('not', '(', $.identifier, optional(seq('where', $._expression)), ')'),
        fork_op: $ => seq('fork', '(', commaSep1($.fork_path), ')'),
        any_op: $ => seq('any', '(', optional($.integer), ')'),
        all_op: $ => seq('all', '(', ')'),
        first_op: $ => seq('first', '(', ')'),
        forecast_op: $ => seq('forecast', '(', optional(commaSep1($.named_argument)), ')'),
        enrich_op: $ => seq('enrich', '(', $.identifier, ',', commaSep1($.named_argument), ')'),

        followed_by_operation: $ => seq(
            '->',
            optional('all'),
            field('event_type', $.identifier),
            optional(seq('where', field('filter', $._expression))),
            optional(seq('as', field('alias', $.identifier))),
        ),

        select_item: $ => choice(
            seq(field('alias', $.identifier), ':', field('expression', $._expression)),
            field('field', $.identifier),
        ),

        window_args: $ => seq(
            field('duration', $._expression),
            optional(seq(',', 'sliding', ':', field('sliding', $._expression))),
            optional(seq(',', 'policy', ':', field('policy', $._expression))),
        ),

        agg_item: $ => seq(
            field('alias', $.identifier),
            ':',
            field('expression', $._expression),
        ),

        order_item: $ => seq(
            field('expression', $._expression),
            optional(choice('asc', 'desc')),
        ),

        pattern_def: $ => seq(
            field('name', $.identifier),
            ':',
            field('matcher', $._expression),
        ),

        fork_path: $ => seq(
            field('name', $.identifier),
            ':',
            repeat1($.stream_operation),
        ),

        named_argument: $ => seq(
            field('name', $.identifier),
            ':',
            field('value', $._expression),
        ),

        // ========================================================================
        // Event Declaration
        // ========================================================================

        event_declaration: $ => seq(
            'event',
            field('name', $.identifier),
            optional(seq('extends', field('parent', $.identifier))),
            ':',
            repeat1($.field_declaration),
        ),

        field_declaration: $ => seq(
            field('name', $.identifier),
            ':',
            field('type', $._type),
            optional('?'),
        ),

        // ========================================================================
        // Type Declaration
        // ========================================================================

        type_declaration: $ => seq(
            'type',
            field('name', $.identifier),
            '=',
            field('type', $._type),
        ),

        // ========================================================================
        // Variable Declaration
        // ========================================================================

        variable_declaration: $ => seq(
            choice('let', 'var'),
            field('name', $.identifier),
            optional(seq(':', field('type', $._type))),
            '=',
            field('value', $._expression),
        ),

        constant_declaration: $ => seq(
            'const',
            field('name', $.identifier),
            optional(seq(':', field('type', $._type))),
            '=',
            field('value', $._expression),
        ),

        // ========================================================================
        // Function Declaration
        // ========================================================================

        function_declaration: $ => seq(
            'fn',
            field('name', $.identifier),
            '(',
            optional(commaSep1($.parameter)),
            ')',
            optional(seq('->', field('return_type', $._type))),
            ':',
            repeat1($._statement),
        ),

        parameter: $ => seq(
            field('name', $.identifier),
            ':',
            field('type', $._type),
        ),

        // ========================================================================
        // Control Flow
        // ========================================================================

        if_statement: $ => seq(
            'if',
            field('condition', $._expression),
            ':',
            repeat1($._statement),
            repeat($.elif_clause),
            optional($.else_clause),
        ),

        elif_clause: $ => seq(
            'elif',
            field('condition', $._expression),
            ':',
            repeat1($._statement),
        ),

        else_clause: $ => seq(
            'else',
            ':',
            repeat1($._statement),
        ),

        for_statement: $ => seq(
            'for',
            field('variable', $.identifier),
            'in',
            field('iterable', $._expression),
            ':',
            repeat1($._statement),
        ),

        while_statement: $ => seq(
            'while',
            field('condition', $._expression),
            ':',
            repeat1($._statement),
        ),

        return_statement: $ => seq(
            'return',
            optional(field('value', $._expression)),
        ),

        break_statement: $ => 'break',
        continue_statement: $ => 'continue',

        expression_statement: $ => $._expression,

        // ========================================================================
        // Config
        // ========================================================================

        config_block: $ => seq(
            'config',
            ':',
            repeat1($.config_item),
        ),

        config_item: $ => seq(
            field('key', $.identifier),
            ':',
            field('value', $._config_value),
        ),

        _config_value: $ => choice(
            $.integer,
            $.float,
            $.string,
            $.duration,
            $.boolean,
            $.identifier,
        ),

        // ========================================================================
        // Import
        // ========================================================================

        import_statement: $ => seq(
            'import',
            field('path', $.string),
            optional(seq('as', field('alias', $.identifier))),
        ),

        // ========================================================================
        // Types
        // ========================================================================

        _type: $ => choice(
            $.primitive_type,
            $.array_type,
            $.map_type,
            $.tuple_type,
            $.optional_type,
            $.stream_type,
            $.named_type,
        ),

        primitive_type: $ => choice('int', 'float', 'bool', 'str', 'timestamp', 'duration'),
        array_type: $ => seq('[', $._type, ']'),
        map_type: $ => seq('{', $._type, ':', $._type, '}'),
        tuple_type: $ => seq('(', commaSep1($._type), ')'),
        optional_type: $ => seq($._type, '?'),
        stream_type: $ => seq('Stream', '<', $._type, '>'),
        named_type: $ => $.identifier,

        // ========================================================================
        // Expressions
        // ========================================================================

        _expression: $ => choice(
            $.lambda_expression,
            $.if_expression,
            $.or_expression,
        ),

        lambda_expression: $ => prec.right('lambda', seq(
            choice(
                seq('(', optional(commaSep1($.identifier)), ')'),
                $.identifier,
            ),
            '=>',
            field('body', $._expression),
        )),

        if_expression: $ => prec.right(seq(
            'if',
            field('condition', $._expression),
            'then',
            field('then', $._expression),
            'else',
            field('else', $._expression),
        )),

        or_expression: $ => choice(
            prec.left('or', seq($._expression, 'or', $._expression)),
            $.and_expression,
        ),

        and_expression: $ => choice(
            prec.left('and', seq($._expression, 'and', $._expression)),
            $.not_expression,
        ),

        not_expression: $ => choice(
            prec('not', seq('not', $._expression)),
            $.comparison_expression,
        ),

        comparison_expression: $ => choice(
            prec.left('comparison', seq($._expression, $.comparison_operator, $._expression)),
            $.bitwise_or_expression,
        ),

        comparison_operator: $ => choice('==', '!=', '<', '<=', '>', '>=', 'in', 'is'),

        bitwise_or_expression: $ => choice(
            prec.left('bitwise_or', seq($._expression, '|', $._expression)),
            $.bitwise_xor_expression,
        ),

        bitwise_xor_expression: $ => choice(
            prec.left('bitwise_xor', seq($._expression, '^', $._expression)),
            $.bitwise_and_expression,
        ),

        bitwise_and_expression: $ => choice(
            prec.left('bitwise_and', seq($._expression, '&', $._expression)),
            $.shift_expression,
        ),

        shift_expression: $ => choice(
            prec.left('shift', seq($._expression, choice('<<', '>>'), $._expression)),
            $.additive_expression,
        ),

        additive_expression: $ => choice(
            prec.left('additive', seq($._expression, choice('+', '-'), $._expression)),
            $.multiplicative_expression,
        ),

        multiplicative_expression: $ => choice(
            prec.left('multiplicative', seq($._expression, choice('*', '/', '%'), $._expression)),
            $.power_expression,
        ),

        power_expression: $ => choice(
            prec.right('power', seq($._expression, '**', $._expression)),
            $.unary_expression,
        ),

        unary_expression: $ => choice(
            prec('unary', seq(choice('-', '~'), $._expression)),
            $.postfix_expression,
        ),

        postfix_expression: $ => choice(
            $.member_expression,
            $.optional_member_expression,
            $.index_expression,
            $.call_expression,
            $.primary_expression,
        ),

        member_expression: $ => prec.left(seq(
            field('object', $._expression),
            '.',
            field('member', $.identifier),
        )),

        optional_member_expression: $ => prec.left(seq(
            field('object', $._expression),
            '?.',
            field('member', $.identifier),
        )),

        index_expression: $ => prec.left(seq(
            field('object', $._expression),
            '[',
            field('index', $._expression),
            ']',
        )),

        call_expression: $ => prec.left(seq(
            field('function', $._expression),
            '(',
            optional(commaSep1($.argument)),
            ')',
        )),

        argument: $ => choice(
            $.named_argument,
            $._expression,
        ),

        primary_expression: $ => choice(
            $.identifier,
            $.integer,
            $.float,
            $.string,
            $.duration,
            $.timestamp,
            $.boolean,
            $.null,
            $.array_literal,
            $.map_literal,
            $.parenthesized_expression,
        ),

        parenthesized_expression: $ => seq('(', $._expression, ')'),

        array_literal: $ => seq('[', optional(commaSep1($._expression)), ']'),

        map_literal: $ => seq('{', optional(commaSep1($.map_entry)), '}'),

        map_entry: $ => seq(
            field('key', choice($.string, $.identifier)),
            ':',
            field('value', $._expression),
        ),

        // ========================================================================
        // Literals and Identifiers
        // ========================================================================

        identifier: $ => /[a-zA-Z_][a-zA-Z0-9_]*/,

        integer: $ => /[0-9]+/,

        float: $ => /[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?/,

        string: $ => choice(
            seq('"', /[^"]*/, '"'),
            seq("'", /[^']*/, "'"),
        ),

        duration: $ => /[0-9]+(ns|us|ms|s|m|h|d)/,

        timestamp: $ => /@[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})?)?/,

        boolean: $ => choice('true', 'false'),

        null: $ => 'null',

        comment: $ => choice(
            seq('#', /.*/),
            seq('/*', /[^*]*\*+([^/*][^*]*\*+)*/, '/'),
        ),
    },
});

// Helper function for comma-separated lists
function commaSep1(rule) {
    return seq(rule, repeat(seq(',', rule)));
}

function commaSep(rule) {
    return optional(commaSep1(rule));
}
