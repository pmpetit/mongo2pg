#!/bin/bash

URI="mongodb://user:pass@localhost:27017/?authSource=admin&directConnection=true"
DB_NAME="sample_airbnb"
COLL_NAME="listingsAndReviews"
INTERVAL=2

echo "Starting strict MongoDB traffic simulation..."
echo "Press [CTRL+C] to stop."
echo "----------------------------------------------------------------"

while true; do
    OP=$(( ( RANDOM % 3 ) + 1 ))
    RAND_ID=$(( ( RANDOM % 90000000 ) + 10000000 ))

    case $OP in
        1)
            echo "[INSERT] Creating strict document matching PostgreSQL constraints..."
            mongosh "$URI" --quiet --eval "
                db.getSiblingDB('${DB_NAME}').${COLL_NAME}.insertOne({
                    _id: '${RAND_ID}',
                    listing_url: 'https://www.airbnb.com/rooms/${RAND_ID}',
                    name: 'Strict Constraint Flat',
                    summary: 'Generated dynamically complying with SQL NOT NULL schemas.',
                    space: 'A small cozy room.',
                    description: 'Full description of the strict constraint flat setup.',
                    neighborhood_overview: 'Safe neighborhood.',
                    notes: 'No special notes.',
                    transit: 'Close to public subway station.',
                    access: '', 
                    interaction: 'Host available via app text.',
                    house_rules: 'Keep it clean and respect neighbors.',
                    property_type: 'Apartment',
                    room_type: 'Entire home/apt',
                    bed_type: 'Real Bed',
                    minimum_nights: '2',
                    maximum_nights: '1125',
                    cancellation_policy: 'flexible',
                    last_scraped: new Date(),
                    calendar_last_scraped: new Date(),
                    accommodates: NumberInt('2'),
                    bedrooms: NumberInt('1'),
                    beds: NumberInt('1'),
                    number_of_reviews: NumberInt('0'),
                    bathrooms: NumberDecimal('1.0'),
                    amenities: ['Wifi', 'Kitchen', 'Essentials'],
                    price: NumberDecimal('250.00'),
                    extra_people: NumberDecimal('0.00'),
                    guests_included: NumberDecimal('1'),
                    images: {
                        thumbnail_url: '',
                        medium_url: '',
                        picture_url: 'https://a0.muscache.com/im/pictures/default.jpg',
                        xl_picture_url: ''
                    },
                    host: {
                        host_id: '9999999',
                        host_url: 'https://www.airbnb.com/users/show/9999999',
                        host_name: 'System Simulator',
                        host_location: 'Rio de Janeiro, Brazil',
                        host_about: 'Automated script host profile.',
                        host_thumbnail_url: 'https://a0.muscache.com/im/pictures/profile.jpg',
                        host_picture_url: 'https://a0.muscache.com/im/pictures/profile.jpg',
                        host_neighbourhood: 'Jardim Botânico',
                        host_is_superhost: false,
                        host_has_profile_pic: true,
                        host_identity_verified: false,
                        host_listings_count: NumberInt('1'),
                        host_total_listings_count: NumberInt('1'),
                        host_verifications: ['email', 'phone']
                    },
                    address: {
                        street: 'Rio de Janeiro, Brazil',
                        suburb: 'Jardim Botânico',
                        government_area: 'Jardim Botânico',
                        market: 'Rio De Janeiro',
                        country: 'Brazil',
                        country_code: 'BR',
                        location: {
                            type: 'Point',
                            coordinates: [Double('-43.2307'), Double('-22.9662')],
                            is_location_exact: true
                        }
                    },
                    availability: {
                        availability_30: NumberInt('15'),
                        availability_60: NumberInt('30'),
                        availability_90: NumberInt('45'),
                        availability_365: NumberInt('180')
                    },
                    review_scores: {},
                    reviews: []
                });
            "
            ;;
            
        2)
            echo "[UPDATE] Updating variables that have safe fallback constraints..."
            mongosh "$URI" --quiet --eval "
                var randomDoc = db.getSiblingDB('${DB_NAME}').${COLL_NAME}.aggregate([{ \$sample: { size: 1 } }]).toArray()[0];
                if (randomDoc) {
                    var newPrice = (Math.random() * 400 + 100).toFixed(2);
                    var newAvail30 = Math.floor(Math.random() * 30);
                    
                    db.getSiblingDB('${DB_NAME}').${COLL_NAME}.updateOne(
                        { _id: randomDoc._id },
                        { 
                            \$set: { 
                                price: NumberDecimal(newPrice),
                                'availability.availability_30': NumberInt(newAvail30),
                                'availability.availability_60': NumberInt(newAvail30 * 2),
                                'availability.availability_90': NumberInt(newAvail30 * 3),
                                'availability.availability_365': NumberInt(newAvail30 * 12)
                            } 
                        }
                    );
                    print('-> Updated constraints on ID: ' + randomDoc._id + ' | Price: $' + newPrice);
                } else {
                    print('-> No documents available to update.');
                }
            "
            ;;
            
        3)
            echo "[DELETE] Executing document eviction..."
            mongosh "$URI" --quiet --eval "
                var randomDoc = db.getSiblingDB('${DB_NAME}').${COLL_NAME}.aggregate([{ \$sample: { size: 1 } }]).toArray()[0];
                if (randomDoc) {
                    db.getSiblingDB('${DB_NAME}').${COLL_NAME}.deleteOne({ _id: randomDoc._id });
                    print('-> Successfully removed listing ID: ' + randomDoc._id);
                } else {
                    print('-> No records available to wipe.');
                }
            "
            ;;
    esac

    echo "Sleeping for ${INTERVAL} seconds..."
    echo "----------------------------------------"
    sleep $INTERVAL
done