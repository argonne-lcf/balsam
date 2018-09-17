$(document).ready( 
    function() 
    {
        // Pre-check boxes
        $(".pre-check").prop('checked', "true");

        // Replace footer with search boxes
        $('#table_id tfoot th').each( function () {
            $(this).html('<input type="text" class="form-control" placeholder="Search"/>');
        } );

        // Generate table
        var table = $('#table_id').DataTable(
        {
            "dom": "<'row'<'col-md-4 col-sm-6'l><'col-md-4 col-sm-6'i><'col-md-4 col-sm-12'p>>"+"tr",
            "ajax": "/balsam/api/tasks_list",
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
        var idx = $(this).attr('data-column');
        var show = $(this).prop('checked');
        var column = table.column(idx);
        column.visible(show);
        table.columns.adjust().draw()
    });

    // Toggle column visibility
    $("#SelectCols >> .form-check-input").on("click", function (e) {
        var idx = $(this).attr('data-column')
        var column = table.column(idx);
        var show = $(this).prop('checked') 
        column.visible(show);
        table.columns.adjust().draw()
    });
} );
