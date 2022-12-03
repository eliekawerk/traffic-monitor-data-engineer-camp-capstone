from faker.providers import DynamicProvider

# Prepare additional faker providers
car_colour_provider = DynamicProvider(
     provider_name="car_colour",
     elements=["white", "sliver", "black", "red", "blue", "yellow","green"],
)

car_fuel_type_provider = DynamicProvider(
     provider_name="car_fuel_type",
     elements=["petrol", "diesel", "electric"],
)

car_passengers_count_provider = DynamicProvider(
     provider_name="car_passengers_count",
     elements=[1, 2, 3, 4, 5]
)

car_travel_direction_provider = DynamicProvider(
     provider_name="car_travel_direction",
     elements=["Northbound", "Southbound", "Eastbound", "Westbound"]
)
