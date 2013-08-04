package ca.kijiji.contest.parsers

import util.parsing.combinator._

object LocationParser extends JavaTokenParsers{
  lazy val number = """[0-9\W]*""".r
  lazy val suffix = """(STREET|AVE|COURT|ST|AV|CRT|CT|RD)\W*""".r
  lazy val direction = """(NORTH|SOUTH|EAST|WEST|N|S|E|W)\W*""".r
  lazy val name = """[\w'-/]+\b""".r  ~
  rep("""(?!(?:ST|STREET|AV|AVE|COURT|CRT|CT|RD)(?:\b+|\W+|$))(?!(?:NORTH|SOUTH|EAST|WEST|N|S|E|W)(?:$|\W+))\b[\w'-/]+""".r) ^^ {
    case firstWord ~ rest =>{
      firstWord::rest
    }
  }

  lazy val spaces = """\b""".r
  lazy val others = """.*""".r

  lazy val location:Parser[List[String]] = opt(number) ~> name <~ opt(suffix) <~ opt(direction) <~ opt(others) ^^{
    case nameToken => {
        nameToken
    }
  }



  def parse(input:String):List[String] = parseAll(location, input).getOrElse(List():List[String]) 


  
}
