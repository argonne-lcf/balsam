function add_column(colname)
{
    $("#SelectCols").append(
        '<div class="form-check"> ' +
        '<input class="form-check-input col-view-check" type="checkbox" column-name='+colname+'>' +
        '<label class="form-check-label">' + colname + '</label>' +
        '</div>');
    $("#tasks_table thead tr").append('<th>'+colname+'</th>');
    $("#tasks_table tfoot tr").append('<input type="text" class="form-control" placeholder="Search"/>');
}

function setup_table(json)
{
    // Generate table skeleton
    for (var i = 0; i < json.columns.length; i++)
    {
        var colName = json.columns[i];
        add_column(colName);
    }

    $(".col-view-check").prop('checked', "true"); // Pre-check boxes

    // Generate table
    var table = $('#tasks_table').DataTable(
    {
        "dom": "<'row'<'col-md-4 col-sm-6'l><'col-md-4 col-sm-6'i><'col-md-4 col-sm-12'p>>"+"tr",
        "data": json.data,
        "serverSide": false,
        "deferRender": true,
        "scrollX": true,
        "scrollY": "67vh",
        "scrollCollapse": true,
        "oLanguage": {
            "sInfo": "Showing _START_ to _END_ of _TOTAL_ tasks",
            "sLengthMenu": "Display _MENU_ tasks"
        },
        "columnDefs": [
            {
                "render": function (data, type, row) {
                    return data.substring(0,8);
                },
                "targets": 0
            },
        ],
        "fnInitComplete": function() {
            this.fnAdjustColumnSizing(true);
            table.columns.adjust().draw()
        },
    });
        
    // Apply by-column search
    table.columns().every( function () {
        var that = this;
        var timer = 0;
        $( 'input', this.footer() ).on('keyup change', function () {
            if ( that.search() !== this.value ) {
                clearTimeout(timer);
                timer = setTimeout(that.search( this.value ).draw, 350);
            }
        } );
    } );

    // Pre-define column visibility
    $("#SelectCols >> .form-check-input").each(function (e) {
        var name = $(this).attr('column-name');
        var show = $(this).prop('checked');
        var column = table.column(name+':name');
        column.visible(show);
        table.columns.adjust().draw()
    });

    // Toggle column visibility
    $("#SelectCols >> .form-check-input").on("click", function (e) {
        var name = $(this).attr('column-name');
        var column = table.column(name+':name');
        var show = $(this).prop('checked') 
        column.visible(show);
        table.columns.adjust().draw()
    });
}

$(document).ready(
    function() {
        $.ajax({
            url: "/balsam/api/tasks_list",
            success: setup_table,
        });
    }
);
