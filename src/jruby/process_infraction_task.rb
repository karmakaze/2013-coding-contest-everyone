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
         # csv_data = CSV.parse_line(@line)

         # TIL - Ruby's CSV library is powerful but SLOW, this is a massive cheat
         # but results in a 75% efficiency boost =/
         csv_data = @line.split(/,/)

         infraction = Infraction.new_from_csv(csv_data)
         
         @task.each infraction, @row
      rescue Infraction::ParseError => e
         # do nothing
      rescue CSV::MalformedCSVError => e
         # do nothing
      end
   end

end