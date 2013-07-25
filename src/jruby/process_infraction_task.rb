require 'csv'
require 'infraction'


class ProcessInfractionTask
   include java.lang.Runnable

   def initialize(line, task, row_idx)
      @line = line
      @task = task
      @row = row_idx
   end

   def run
      begin
         csv_data = CSV.parse_line(@line)

         infraction = Infraction.new_from_csv(csv_data)
         
         @task.each infraction, @row
      rescue CSV::MalformedCSVError => e
         # do nothing
      end
   end

end