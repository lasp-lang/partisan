{application, app_faulty,
 [{description, "A faulty application used in supervisor_SUITE:faulty_application_shutdown/1"},
  {vsn, "1.0"},
  {modules, [app_faulty, app_faulty_sup, app_faulty_server]},
  {registered, [app_faulty]},
  {applications, [kernel, stdlib]},
  {mod, {app_faulty, []}}
 ]}.
