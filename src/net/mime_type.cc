#include <stdexcept>
#include <string_view>

#include "mime_type.hh"
#include "split.hh"

using namespace std;

MIMEType::MIMEType( const string_view content_type )
  : type_()
  , parameters_()
{
  vector<string_view> type_and_parameters;
  split( content_type, ';', type_and_parameters );
  if ( type_and_parameters.size() == 0 or type_and_parameters.at( 0 ).empty() ) {
    throw runtime_error( "MIMEType: invalid MIME media-type string" );
  }

  type_ = type_and_parameters.at( 0 );
  /* XXX don't parse the parameters for now */
}
